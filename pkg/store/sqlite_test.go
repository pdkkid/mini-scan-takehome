package store_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/censys/scan-takehome/pkg/store"
)

func newTestStore(t *testing.T) *store.SQLiteStore {
	t.Helper()
	s, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("failed to close store: %v", err)
		}
	})
	return s
}

func TestUpsert_Insert(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rec := store.ScanRecord{
		IP:          "1.1.1.1",
		Port:        80,
		Service:     "HTTP",
		LastScanned: 1000,
		Response:    "hello",
	}

	if err := s.Upsert(ctx, rec); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	got, err := s.Get(ctx, rec.IP, rec.Port, rec.Service)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil {
		t.Fatal("expected record, got nil")
	}
	if got.Response != "hello" {
		t.Errorf("Response: got %q, want %q", got.Response, "hello")
	}
	if got.LastScanned != 1000 {
		t.Errorf("LastScanned: got %d, want 1000", got.LastScanned)
	}
}

func TestUpsert_UpdateNewer(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	older := store.ScanRecord{IP: "1.1.1.1", Port: 80, Service: "HTTP", LastScanned: 500, Response: "old"}
	newer := store.ScanRecord{IP: "1.1.1.1", Port: 80, Service: "HTTP", LastScanned: 1000, Response: "new"}

	if err := s.Upsert(ctx, older); err != nil {
		t.Fatalf("Upsert older: %v", err)
	}
	if err := s.Upsert(ctx, newer); err != nil {
		t.Fatalf("Upsert newer: %v", err)
	}

	got, err := s.Get(ctx, "1.1.1.1", 80, "HTTP")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Response != "new" {
		t.Errorf("Response: got %q, want %q", got.Response, "new")
	}
	if got.LastScanned != 1000 {
		t.Errorf("LastScanned: got %d, want 1000", got.LastScanned)
	}
}

func TestUpsert_RejectOlder(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	newer := store.ScanRecord{IP: "1.1.1.1", Port: 80, Service: "HTTP", LastScanned: 1000, Response: "newer"}
	older := store.ScanRecord{IP: "1.1.1.1", Port: 80, Service: "HTTP", LastScanned: 500, Response: "older"}

	if err := s.Upsert(ctx, newer); err != nil {
		t.Fatalf("Upsert newer: %v", err)
	}
	// Older scan arrives late — should be a no-op.
	if err := s.Upsert(ctx, older); err != nil {
		t.Fatalf("Upsert older: %v", err)
	}

	got, err := s.Get(ctx, "1.1.1.1", 80, "HTTP")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Response != "newer" {
		t.Errorf("Response: got %q, want %q (old scan should not overwrite newer)", got.Response, "newer")
	}
	if got.LastScanned != 1000 {
		t.Errorf("LastScanned: got %d, want 1000", got.LastScanned)
	}
}

func TestUpsert_SameTimestamp(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	first := store.ScanRecord{IP: "1.1.1.1", Port: 80, Service: "HTTP", LastScanned: 1000, Response: "first"}
	dup := store.ScanRecord{IP: "1.1.1.1", Port: 80, Service: "HTTP", LastScanned: 1000, Response: "duplicate"}

	if err := s.Upsert(ctx, first); err != nil {
		t.Fatalf("Upsert first: %v", err)
	}
	// Duplicate with same timestamp should be a no-op (strict > check).
	if err := s.Upsert(ctx, dup); err != nil {
		t.Fatalf("Upsert dup: %v", err)
	}

	got, err := s.Get(ctx, "1.1.1.1", 80, "HTTP")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Response != "first" {
		t.Errorf("Response: got %q, want %q (same-timestamp duplicate should not overwrite)", got.Response, "first")
	}
}

func TestGet_NotFound(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	got, err := s.Get(ctx, "9.9.9.9", 443, "DNS")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for unknown key, got %+v", got)
	}
}

func TestUpsert_IsolatesByKey(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	records := []store.ScanRecord{
		{IP: "1.1.1.1", Port: 80, Service: "HTTP", LastScanned: 100, Response: "http"},
		{IP: "1.1.1.1", Port: 22, Service: "SSH", LastScanned: 200, Response: "ssh"},
		{IP: "1.1.1.2", Port: 80, Service: "HTTP", LastScanned: 300, Response: "http2"},
	}

	for _, r := range records {
		if err := s.Upsert(ctx, r); err != nil {
			t.Fatalf("Upsert %+v: %v", r, err)
		}
	}

	for _, want := range records {
		got, err := s.Get(ctx, want.IP, want.Port, want.Service)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got == nil {
			t.Fatalf("expected record for %s:%d/%s, got nil", want.IP, want.Port, want.Service)
		}
		if got.Response != want.Response {
			t.Errorf("Response for %s:%d/%s: got %q, want %q", want.IP, want.Port, want.Service, got.Response, want.Response)
		}
	}
}

func TestUpsert_Concurrent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)

	// Each goroutine upserts with a different timestamp. The final stored
	// record must reflect the highest timestamp seen.
	for i := 0; i < goroutines; i++ {
		ts := int64(i + 1)
		go func(ts int64) {
			defer wg.Done()
			_ = s.Upsert(ctx, store.ScanRecord{
				IP:          "1.1.1.1",
				Port:        80,
				Service:     "HTTP",
				LastScanned: ts,
				Response:    fmt.Sprintf("response-%d", ts),
			})
		}(ts)
	}

	wg.Wait()

	got, err := s.Get(ctx, "1.1.1.1", 80, "HTTP")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil {
		t.Fatal("expected record, got nil")
	}
	if got.LastScanned != goroutines {
		t.Errorf("LastScanned: got %d, want %d (highest timestamp)", got.LastScanned, goroutines)
	}
}
