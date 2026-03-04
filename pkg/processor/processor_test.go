package processor_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"

	"cloud.google.com/go/pubsub"
	"github.com/censys/scan-takehome/pkg/processor"
	"github.com/censys/scan-takehome/pkg/scanning"
	"github.com/censys/scan-takehome/pkg/store"
)

// errStore is a Store that always fails Upsert, simulating a transient
// outage (e.g. database unavailable).
type errStore struct{}

func (e *errStore) Upsert(_ context.Context, _ store.ScanRecord) error {
	return errors.New("store unavailable")
}
func (e *errStore) Get(_ context.Context, _ string, _ uint32, _ string) (*store.ScanRecord, error) {
	return nil, errors.New("store unavailable")
}
func (e *errStore) Ping(_ context.Context) error { return errors.New("store unavailable") }
func (e *errStore) Close() error                 { return nil }

// makeMsgData marshals a Scan into the JSON bytes the scanner would publish.
func makeMsgData(t *testing.T, scan *scanning.Scan) []byte {
	t.Helper()
	data, err := json.Marshal(scan)
	if err != nil {
		t.Fatalf("json.Marshal scan: %v", err)
	}
	return data
}

func TestProcess_V2(t *testing.T) {
	ms := store.NewMemoryStore()
	p := processor.New(ms)
	ctx := context.Background()

	scan := &scanning.Scan{
		Ip:          "1.1.1.1",
		Port:        80,
		Service:     "HTTP",
		Timestamp:   1000,
		DataVersion: scanning.V2,
		Data:        &scanning.V2Data{ResponseStr: "hello world"},
	}

	msg := &pubsub.Message{ID: "test-1", Data: makeMsgData(t, scan)}
	p.HandleMessage(ctx, msg)

	got, err := ms.Get(ctx, "1.1.1.1", 80, "HTTP")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil {
		t.Fatal("expected record, got nil")
	}
	if got.Response != "hello world" {
		t.Errorf("Response: got %q, want %q", got.Response, "hello world")
	}
	if got.LastScanned != 1000 {
		t.Errorf("LastScanned: got %d, want 1000", got.LastScanned)
	}
}

func TestProcess_V1(t *testing.T) {
	ms := store.NewMemoryStore()
	p := processor.New(ms)
	ctx := context.Background()

	scan := &scanning.Scan{
		Ip:          "1.1.1.1",
		Port:        22,
		Service:     "SSH",
		Timestamp:   2000,
		DataVersion: scanning.V1,
		Data:        &scanning.V1Data{ResponseBytesUtf8: []byte("service response: 42")},
	}

	msg := &pubsub.Message{ID: "test-2", Data: makeMsgData(t, scan)}
	p.HandleMessage(ctx, msg)

	got, err := ms.Get(ctx, "1.1.1.1", 22, "SSH")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil {
		t.Fatal("expected record, got nil")
	}
	if got.Response != "service response: 42" {
		t.Errorf("Response: got %q, want %q", got.Response, "service response: 42")
	}
}

// TestProcess_V1_HelloWorld verifies the exact example from the README:
// base64("hello world") == "aGVsbG8gd29ybGQ=" and the decoded response is "hello world".
func TestProcess_V1_HelloWorld(t *testing.T) {
	ms := store.NewMemoryStore()
	p := processor.New(ms)
	ctx := context.Background()

	// Verify the base64 encoding matches the README example.
	encoded := base64.StdEncoding.EncodeToString([]byte("hello world"))
	if encoded != "aGVsbG8gd29ybGQ=" {
		t.Fatalf("unexpected base64: %s", encoded)
	}

	scan := &scanning.Scan{
		Ip:          "1.1.1.1",
		Port:        53,
		Service:     "DNS",
		Timestamp:   3000,
		DataVersion: scanning.V1,
		Data:        &scanning.V1Data{ResponseBytesUtf8: []byte("hello world")},
	}

	msg := &pubsub.Message{ID: "test-3", Data: makeMsgData(t, scan)}
	p.HandleMessage(ctx, msg)

	got, err := ms.Get(ctx, "1.1.1.1", 53, "DNS")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil {
		t.Fatal("expected record, got nil")
	}
	if got.Response != "hello world" {
		t.Errorf("Response: got %q, want %q", got.Response, "hello world")
	}
}

// TestProcess_UnknownVersion verifies that an unrecognised data_version is
// classified as a permanent error. The message is Acked (dropped) rather than
// Nacked to prevent an infinite redelivery loop — retrying will never help.
func TestProcess_UnknownVersion(t *testing.T) {
	ms := store.NewMemoryStore()
	p := processor.New(ms)
	ctx := context.Background()

	scan := &scanning.Scan{
		Ip:          "1.1.1.1",
		Port:        80,
		Service:     "HTTP",
		Timestamp:   4000,
		DataVersion: scanning.Version, // == 0, not V1 or V2
		Data:        map[string]interface{}{},
	}

	err := p.Process(ctx, makeMsgData(t, scan))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, processor.ErrPermanent) {
		t.Errorf("unknown version should be a permanent error, got: %v", err)
	}

	// Nothing should have been stored.
	got, _ := ms.Get(ctx, "1.1.1.1", 80, "HTTP")
	if got != nil {
		t.Errorf("expected no record for unknown version, got %+v", got)
	}
}

// TestProcess_MalformedJSON verifies that unparseable payloads are classified
// as permanent errors. Retrying malformed bytes will always fail, so the
// message is Acked to drop it rather than Nacked to loop forever.
func TestProcess_MalformedJSON(t *testing.T) {
	ms := store.NewMemoryStore()
	p := processor.New(ms)
	ctx := context.Background()

	err := p.Process(ctx, []byte("not valid json {"))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, processor.ErrPermanent) {
		t.Errorf("malformed JSON should be a permanent error, got: %v", err)
	}

	got, _ := ms.Get(ctx, "1.1.1.1", 80, "HTTP")
	if got != nil {
		t.Errorf("expected no record after malformed JSON, got %+v", got)
	}
}

// TestProcess_TransientStoreError verifies that a store failure is NOT
// classified as permanent. Pub/Sub will Nack and redeliver the message
// so it can succeed once the store recovers.
func TestProcess_TransientStoreError(t *testing.T) {
	p := processor.New(&errStore{})
	ctx := context.Background()

	scan := &scanning.Scan{
		Ip:          "1.1.1.1",
		Port:        80,
		Service:     "HTTP",
		Timestamp:   5000,
		DataVersion: scanning.V2,
		Data:        &scanning.V2Data{ResponseStr: "hello"},
	}

	err := p.Process(ctx, makeMsgData(t, scan))
	if err == nil {
		t.Fatal("expected store error, got nil")
	}
	if errors.Is(err, processor.ErrPermanent) {
		t.Errorf("store error should be transient (not permanent), got: %v", err)
	}
}
