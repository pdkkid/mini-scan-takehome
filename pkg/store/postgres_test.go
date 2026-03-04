package store_test

import (
	"context"
	"os"
	"testing"

	"github.com/censys/scan-takehome/pkg/store"
)

// TestPostgresStore_Suite runs the full Store behavioral suite against a real
// PostgreSQL instance. Set the POSTGRES_URL environment variable to enable:
//
//	POSTGRES_URL="postgres://processor:processor@localhost:5432/scans?sslmode=disable" go test -race ./pkg/store/...
//
// When POSTGRES_URL is not set the test is skipped, so `go test ./...`
// continues to work without a running Postgres instance.
func TestPostgresStore_Suite(t *testing.T) {
	connStr := os.Getenv("POSTGRES_URL")
	if connStr == "" {
		t.Skip("POSTGRES_URL not set; skipping Postgres integration tests")
	}

	runStoreSuite(t, func(t *testing.T) store.Store {
		ctx := context.Background()

		s, err := store.NewPostgresStore(ctx, connStr)
		if err != nil {
			t.Fatalf("NewPostgresStore: %v", err)
		}

		// Truncate the table to ensure a clean slate for each subtest.
		// SQLite achieves this with t.TempDir() (fresh file per test);
		// Postgres shares one database so we reset explicitly.
		if err := s.Exec(ctx, "TRUNCATE scan_records"); err != nil {
			t.Fatalf("truncate: %v", err)
		}

		t.Cleanup(func() {
			if err := s.Close(); err != nil {
				t.Errorf("failed to close store: %v", err)
			}
		})
		return s
	})
}
