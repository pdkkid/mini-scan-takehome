package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const pgCreateTableSQL = `
CREATE TABLE IF NOT EXISTS scan_records (
    ip           TEXT    NOT NULL,
    port         INTEGER NOT NULL,
    service      TEXT    NOT NULL,
    last_scanned BIGINT  NOT NULL,
    response     TEXT    NOT NULL,
    PRIMARY KEY (ip, port, service)
);`

// The WHERE clause on DO UPDATE ensures we only overwrite when the incoming
// scan is strictly newer than what we have stored — identical semantics to the
// SQLite upsert. PostgreSQL uses $N positional params instead of ?, and BIGINT
// for 64-bit timestamps.
const pgUpsertSQL = `
INSERT INTO scan_records (ip, port, service, last_scanned, response)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (ip, port, service) DO UPDATE SET
    last_scanned = EXCLUDED.last_scanned,
    response     = EXCLUDED.response
WHERE EXCLUDED.last_scanned > scan_records.last_scanned;`

const pgGetSQL = `
SELECT ip, port, service, last_scanned, response
FROM scan_records
WHERE ip = $1 AND port = $2 AND service = $3;`

// PostgresStore is a Store implementation backed by a PostgreSQL database.
// It uses pgxpool for connection pooling, supporting high-concurrency writes
// from multiple goroutines and horizontally-scaled replicas.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore connects to a PostgreSQL database using the provided
// connection string and initializes the schema. The connection string format
// is: postgres://user:password@host:port/dbname?sslmode=disable
func NewPostgresStore(ctx context.Context, connString string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	// Verify connectivity before proceeding with schema migration.
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	// Run schema migration — CREATE TABLE IF NOT EXISTS is idempotent.
	if _, err := pool.Exec(ctx, pgCreateTableSQL); err != nil {
		pool.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}

	return &PostgresStore{pool: pool}, nil
}

// Upsert inserts or conditionally updates a scan record. The existing record
// is only replaced when the incoming LastScanned timestamp is strictly greater
// than the stored one, making this safe for at-least-once delivery and
// arbitrarily out-of-order messages.
func (s *PostgresStore) Upsert(ctx context.Context, record ScanRecord) error {
	_, err := s.pool.Exec(ctx, pgUpsertSQL,
		record.IP,
		record.Port,
		record.Service,
		record.LastScanned,
		record.Response,
	)
	if err != nil {
		return fmt.Errorf("upsert scan record: %w", err)
	}
	return nil
}

// Get retrieves the stored record for the given (ip, port, service) key.
// Returns nil, nil if no record exists.
func (s *PostgresStore) Get(ctx context.Context, ip string, port uint32, service string) (*ScanRecord, error) {
	row := s.pool.QueryRow(ctx, pgGetSQL, ip, port, service)
	r := &ScanRecord{}
	err := row.Scan(&r.IP, &r.Port, &r.Service, &r.LastScanned, &r.Response)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get scan record: %w", err)
	}
	return r, nil
}

// Ping verifies the database connection is alive by issuing a lightweight
// ping through the connection pool.
func (s *PostgresStore) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Close releases all pool connections.
func (s *PostgresStore) Close() error {
	s.pool.Close()
	return nil
}

// Exec runs an arbitrary SQL statement against the pool. This is exported for
// test cleanup (e.g. TRUNCATE between subtests) and is intentionally not part
// of the Store interface.
func (s *PostgresStore) Exec(ctx context.Context, sql string) error {
	_, err := s.pool.Exec(ctx, sql)
	return err
}
