package store_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/censys/scan-takehome/pkg/store"
)

// runStoreSuite executes the full Store behavioral contract against any
// implementation. Both MemoryStore and SQLiteStore must satisfy these
// invariants identically, making them safely interchangeable.
func runStoreSuite(t *testing.T, newStore func(t *testing.T) store.Store) {
	t.Helper()

	t.Run("Upsert/Insert", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		rec := store.ScanRecord{Ip: "1.1.1.1", Port: 80, Service: "HTTP", LastScanned: 1000, Response: "hello"}
		if err := s.Upsert(ctx, rec); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
		got, err := s.Get(ctx, "1.1.1.1", 80, "HTTP")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got == nil {
			t.Fatal("expected record, got nil")
		}
		if got.Response != "hello" || got.LastScanned != 1000 {
			t.Errorf("got %+v, want Response=hello LastScanned=1000", got)
		}
	})

	t.Run("Upsert/UpdateNewer", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		_ = s.Upsert(ctx, store.ScanRecord{Ip: "1.1.1.1", Port: 80, Service: "HTTP", LastScanned: 500, Response: "old"})
		_ = s.Upsert(ctx, store.ScanRecord{Ip: "1.1.1.1", Port: 80, Service: "HTTP", LastScanned: 1000, Response: "new"})

		got, _ := s.Get(ctx, "1.1.1.1", 80, "HTTP")
		if got == nil || got.Response != "new" || got.LastScanned != 1000 {
			t.Errorf("got %+v, want Response=new LastScanned=1000", got)
		}
	})

	t.Run("Upsert/RejectOlder", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		_ = s.Upsert(ctx, store.ScanRecord{Ip: "1.1.1.1", Port: 80, Service: "HTTP", LastScanned: 1000, Response: "newer"})
		_ = s.Upsert(ctx, store.ScanRecord{Ip: "1.1.1.1", Port: 80, Service: "HTTP", LastScanned: 500, Response: "older"})

		got, _ := s.Get(ctx, "1.1.1.1", 80, "HTTP")
		if got == nil || got.Response != "newer" || got.LastScanned != 1000 {
			t.Errorf("late-arriving older scan should not overwrite: got %+v", got)
		}
	})

	t.Run("Upsert/SameTimestamp", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		_ = s.Upsert(ctx, store.ScanRecord{Ip: "1.1.1.1", Port: 80, Service: "HTTP", LastScanned: 1000, Response: "first"})
		_ = s.Upsert(ctx, store.ScanRecord{Ip: "1.1.1.1", Port: 80, Service: "HTTP", LastScanned: 1000, Response: "duplicate"})

		got, _ := s.Get(ctx, "1.1.1.1", 80, "HTTP")
		if got == nil || got.Response != "first" {
			t.Errorf("same-timestamp duplicate should not overwrite: got %+v", got)
		}
	})

	t.Run("Get/NotFound", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		got, err := s.Get(ctx, "9.9.9.9", 443, "DNS")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got != nil {
			t.Errorf("expected nil for unknown key, got %+v", got)
		}
	})

	t.Run("Upsert/IsolatesByKey", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		records := []store.ScanRecord{
			{Ip: "1.1.1.1", Port: 80, Service: "HTTP", LastScanned: 100, Response: "http"},
			{Ip: "1.1.1.1", Port: 22, Service: "SSH", LastScanned: 200, Response: "ssh"},
			{Ip: "1.1.1.2", Port: 80, Service: "HTTP", LastScanned: 300, Response: "http2"},
		}
		for _, r := range records {
			if err := s.Upsert(ctx, r); err != nil {
				t.Fatalf("Upsert: %v", err)
			}
		}
		for _, want := range records {
			got, err := s.Get(ctx, want.Ip, want.Port, want.Service)
			if err != nil || got == nil || got.Response != want.Response {
				t.Errorf("key %s:%d/%s: got %+v, err %v, want Response=%s",
					want.Ip, want.Port, want.Service, got, err, want.Response)
			}
		}
	})

	t.Run("Upsert/Concurrent", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()

		const goroutines = 20
		var wg sync.WaitGroup
		wg.Add(goroutines)
		for i := 0; i < goroutines; i++ {
			ts := int64(i + 1)
			go func(ts int64) {
				defer wg.Done()
				_ = s.Upsert(ctx, store.ScanRecord{
					Ip: "1.1.1.1", Port: 80, Service: "HTTP",
					LastScanned: ts,
					Response:    fmt.Sprintf("response-%d", ts),
				})
			}(ts)
		}
		wg.Wait()

		got, err := s.Get(ctx, "1.1.1.1", 80, "HTTP")
		if err != nil || got == nil || got.LastScanned != goroutines {
			t.Errorf("concurrent upserts: got %+v err %v, want LastScanned=%d", got, err, goroutines)
		}
	})
}

// TestMemoryStore runs the full Store behavioral suite against MemoryStore.
func TestMemoryStore(t *testing.T) {
	runStoreSuite(t, func(t *testing.T) store.Store {
		s := store.NewMemoryStore()
		t.Cleanup(func() { s.Close() })
		return s
	})
}

// TestSQLiteStore_Suite runs the same behavioral suite against SQLiteStore,
// confirming both implementations honour the Store contract identically.
func TestSQLiteStore_Suite(t *testing.T) {
	runStoreSuite(t, func(t *testing.T) store.Store {
		s, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "test.db"))
		if err != nil {
			t.Fatalf("NewSQLiteStore: %v", err)
		}
		t.Cleanup(func() { s.Close() })
		return s
	})
}
