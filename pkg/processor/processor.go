package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/censys/scan-takehome/pkg/metrics"
	"github.com/censys/scan-takehome/pkg/publish"
	"github.com/censys/scan-takehome/pkg/scanning"
	"github.com/censys/scan-takehome/pkg/store"
)

// ErrPermanent is a sentinel used to classify errors that should not be
// retried. Permanent errors indicate the message itself is the
// problem — e.g. malformed JSON or an unknown data version — and will never
// succeed regardless of how many times it is retried.
var ErrPermanent = errors.New("permanent processing error")

// permanent wraps err alongside ErrPermanent so callers can detect the class
// with errors.Is(err, ErrPermanent) while still reading the underlying cause.
func permanent(err error) error {
	return fmt.Errorf("%w: %w", ErrPermanent, err)
}

// Processor subscribes to a Pub/Sub subscription and persists scan results.
type Processor struct {
	store    store.Store
	dlq      publish.Publisher // nil-safe; when nil, permanent errors are dropped
	recorder *metrics.Recorder // nil-safe; omit in tests
}

// New creates a Processor that writes scan records to the given Store.
func New(s store.Store) *Processor {
	return &Processor{store: s}
}

// NewWithRecorder creates a Processor with Prometheus instrumentation.
func NewWithRecorder(s store.Store, r *metrics.Recorder) *Processor {
	return &Processor{store: s, recorder: r}
}

// HandleMessage is a pubsub.MessageHandler that routes messages through
// Process and dispatches Ack or Nack based on the error class:
//
//   - No error        → Ack       (status="ok")
//   - Permanent error → Nack/Ack  (status="permanent"; publish to DQL and Ack, if publishing fails, Nack)
//   - Transient error → Nack      (status="transient"; redeliver when store recovers)
func (p *Processor) HandleMessage(ctx context.Context, msg *pubsub.Message) {
	start := time.Now()
	err := p.Process(ctx, msg.Data)

	var status string
	switch {
	case err == nil:
		// Log structured events for successful processing, including the message ID - mainly to verify working in logs
		// slog.InfoContext(ctx, "message processed successfully",
		// 	"msg_id", msg.ID,
		// 	"processing_time_ms", time.Since(start).Milliseconds(),
		// )
		msg.Ack()
		status = "ok"
	case errors.Is(err, ErrPermanent):
		slog.WarnContext(ctx, "permanent processing error",
			"msg_id", msg.ID,
			"error", err,
		)
		// Forward the original message to the dead letter queue if configured.
		if p.dlq != nil {
			dlqMsg := &pubsub.Message{
				Data: msg.Data,
				Attributes: map[string]string{
					"original_msg_id":       msg.ID,
					"original_publish_time": msg.PublishTime.Format(time.RFC3339Nano),
					"error":                 err.Error(),
				},
			}
			dlqErr := p.dlq.Publish(ctx, dlqMsg)
			if dlqErr != nil {
				slog.ErrorContext(ctx, "failed to publish to DLQ",
					"msg_id", msg.ID,
					"dlq_error", dlqErr,
				)
			} else {
				slog.InfoContext(ctx, "published message to DLQ",
					"msg_id", msg.ID,
				)
				// Increment DLQPublished metric only on successful publish to avoid inflating the metric with failed attempts
				if p.recorder != nil {
					p.recorder.DLQPublished.Inc()
				}
				// Ack the original message only if DLQ publish succeeds; otherwise Nack to trigger redelivery and avoid silent loss
				msg.Ack()
			}
		}
		msg.Nack() // Nack if DLQ publish fails to trigger redelivery and avoid silent message loss
		status = "permanent"
	default:
		slog.WarnContext(ctx, "nacking message (transient error)",
			"msg_id", msg.ID,
			"error", err,
		)
		msg.Nack()
		status = "transient"
	}

	if p.recorder != nil {
		p.recorder.MessagesProcessed.WithLabelValues(status).Inc()
		p.recorder.ProcessingDuration.Observe(time.Since(start).Seconds())
	}
}

// Process handles raw Pub/Sub message bytes and writes the result to the store.
// It is exported to enable white-box testing of error classification.
//
// Parse errors are wrapped with ErrPermanent because a structurally invalid
// message will fail identically on every retry. Store errors are returned
// unwrapped (transient) so HandleMessage Nacks for redelivery.
func (p *Processor) Process(ctx context.Context, data []byte) error {
	var scan scanning.Scan
	if err := json.Unmarshal(data, &scan); err != nil {
		return permanent(fmt.Errorf("unmarshal scan: %w", err))
	}

	response, err := extractResponse(&scan)
	if err != nil {
		return permanent(fmt.Errorf("extract response (data_version=%d): %w", scan.DataVersion, err))
	}

	// Store errors are transient — return unwrapped so HandleMessage Nacks.
	return p.store.Upsert(ctx, store.ScanRecord{
		IP:          scan.Ip,
		Port:        scan.Port,
		Service:     scan.Service,
		LastScanned: scan.Timestamp,
		Response:    response,
	})
}

// SetDLQ configures the dead letter queue publisher. When set, messages that
// produce permanent errors are forwarded to the DLQ instead of being nacked.
// The original message bytes are preserved as-is; error metadata is
// added as Pub/Sub message attributes.
func (p *Processor) SetDLQ(pub publish.Publisher) {
	p.dlq = pub
}

// extractResponse decodes the service response string from either V1 or V2
// data formats. After json.Unmarshal into scanning.Scan, the Data field is a
// map[string]interface{} because the static type is interface{}. We re-marshal
// that map back to JSON bytes and unmarshal into the correct typed struct —
// the idiomatic two-pass pattern for polymorphic JSON in Go.
//
// V1: response_bytes_utf8 is []byte, which encoding/json encodes as a base64
// string on the wire. On re-unmarshal into V1Data, encoding/json automatically
// base64-decodes that string back into []byte, giving us the original bytes.
//
// V2: response_str is a plain string — no transformation required.
func extractResponse(scan *scanning.Scan) (string, error) {
	rawData, err := json.Marshal(scan.Data)
	if err != nil {
		return "", fmt.Errorf("re-marshal data field: %w", err)
	}

	switch scan.DataVersion {
	case scanning.V1:
		var v1 scanning.V1Data
		if err := json.Unmarshal(rawData, &v1); err != nil {
			return "", fmt.Errorf("unmarshal V1Data: %w", err)
		}
		return string(v1.ResponseBytesUtf8), nil

	case scanning.V2:
		var v2 scanning.V2Data
		if err := json.Unmarshal(rawData, &v2); err != nil {
			return "", fmt.Errorf("unmarshal V2Data: %w", err)
		}
		return v2.ResponseStr, nil

	default:
		return "", fmt.Errorf("unknown data_version %d", scan.DataVersion)
	}
}
