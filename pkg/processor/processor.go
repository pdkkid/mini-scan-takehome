package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"cloud.google.com/go/pubsub"
	"github.com/censys/scan-takehome/pkg/scanning"
	"github.com/censys/scan-takehome/pkg/store"
)

// Processor subscribes to a Pub/Sub subscription and persists scan results.
type Processor struct {
	store store.Store
}

// New creates a Processor that writes scan records to the given Store.
func New(s store.Store) *Processor {
	return &Processor{store: s}
}

// HandleMessage is a pubsub.MessageHandler. It implements at-least-once
// semantics: the message is Nacked on any error so Pub/Sub redelivers it,
// and Acked only after a successful store write.
func (p *Processor) HandleMessage(ctx context.Context, msg *pubsub.Message) {
	if err := p.process(ctx, msg.Data); err != nil {
		log.Printf("error processing message id=%s: %v — nacking for redelivery", msg.ID, err)
		msg.Nack()
		return
	}
	msg.Ack()
}

func (p *Processor) process(ctx context.Context, data []byte) error {
	var scan scanning.Scan
	if err := json.Unmarshal(data, &scan); err != nil {
		return fmt.Errorf("unmarshal scan: %w", err)
	}

	response, err := extractResponse(&scan)
	if err != nil {
		return fmt.Errorf("extract response (data_version=%d): %w", scan.DataVersion, err)
	}

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
