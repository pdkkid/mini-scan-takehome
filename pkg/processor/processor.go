package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"cloud.google.com/go/pubsub"
	"github.com/censys/scan-takehome/pkg/scanning"
	"github.com/censys/scan-takehome/pkg/store"
)

// ErrPermanent is a sentinel used to classify errors that should not be
// retried. When a message produces a permanent error, the processor Acks it
// (dropping it from the subscription) rather than Nacking, which would cause
// infinite redelivery. Permanent errors indicate the message itself is the
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
	store store.Store
}

// New creates a Processor that writes scan records to the given Store.
func New(s store.Store) *Processor {
	return &Processor{store: s}
}

// HandleMessage is a pubsub.MessageHandler that routes messages through
// Process and dispatches Ack or Nack based on the error class:
//
//   - No error        → Ack
//   - Permanent error → Ack (drop; retrying will never succeed)
//   - Transient error → Nack (redeliver; e.g. temporary store outage)
func (p *Processor) HandleMessage(ctx context.Context, msg *pubsub.Message) {
	err := p.Process(ctx, msg.Data)
	switch {
	case err == nil:
		msg.Ack()
	case errors.Is(err, ErrPermanent):
		slog.WarnContext(ctx, "dropping message (permanent error)",
			"msg_id", msg.ID,
			"error", err,
		)
		msg.Ack()
	default:
		slog.WarnContext(ctx, "nacking message (transient error)",
			"msg_id", msg.ID,
			"error", err,
		)
		msg.Nack()
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
		Ip:          scan.Ip,
		Port:        scan.Port,
		Service:     scan.Service,
		LastScanned: scan.Timestamp,
		Response:    response,
	})
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
