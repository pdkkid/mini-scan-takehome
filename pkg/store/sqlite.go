package store

import (
	"context"
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

const createTableSQL = `
CREATE TABLE IF NOT EXISTS scan_records (
    ip           TEXT    NOT NULL,
    port         INTEGER NOT NULL,
    service      TEXT    NOT NULL,
    last_scanned INTEGER NOT NULL,
    response     TEXT    NOT NULL,
    PRIMARY KEY (ip, port, service)
);`

// The WHERE clause on DO UPDATE ensures we only overwrite when the incoming
// scan is strictly newer than what we have stored. This single atomic SQL
// operation is the core of our out-of-order protection — no read-before-write
// or application-level locking is required.
const upsertSQL = `
INSERT INTO scan_records (ip, port, service, last_scanned, response)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(ip, port, service) DO UPDATE SET
    last_scanned = excluded.last_scanned,
    response     = excluded.response
WHERE excluded.last_scanned > scan_records.last_scanned;`

const getSQL = `
SELECT ip, port, service, last_scanned, response
FROM scan_records
WHERE ip = ? AND port = ? AND service = ?;`

// SQLiteStore is a Store implementation backed by a local SQLite database.
type SQLiteStore struct {
	db *sql.DB
}

// NewSQLiteStore opens (or creates) a SQLite database at the given path and
// initializes the schema. It configures WAL journal mode and a busy timeout
// to support concurrent access from multiple goroutines (or replicas sharing
// a volume mount).
func NewSQLiteStore(path string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite at %q: %w", path, err)
	}

	// WAL mode allows concurrent readers alongside a single writer, which is
	// important when multiple goroutines (or horizontally-scaled replicas on a
	// shared volume) access the same file simultaneously.
	if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		return nil, fmt.Errorf("set WAL mode: %w", err)
	}

	// Prevent "database is locked" errors when a writer holds the lock.
	if _, err := db.Exec(`PRAGMA busy_timeout=5000`); err != nil {
		return nil, fmt.Errorf("set busy timeout: %w", err)
	}

	// Limit to one open connection. SQLite supports only one concurrent writer;
	// restricting the pool to a single connection serializes writes from
	// concurrent goroutines within this process and avoids SQLITE_BUSY errors.
	db.SetMaxOpenConns(1)

	if _, err := db.Exec(createTableSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}

	return &SQLiteStore{db: db}, nil
}

// Upsert inserts or conditionally updates a scan record. The existing record
// is only replaced when the incoming LastScanned timestamp is strictly greater
// than the stored one, making this safe for at-least-once delivery and
// arbitrarily out-of-order messages.
func (s *SQLiteStore) Upsert(ctx context.Context, record ScanRecord) error {
	_, err := s.db.ExecContext(ctx, upsertSQL,
		record.Ip,
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
func (s *SQLiteStore) Get(ctx context.Context, ip string, port uint32, service string) (*ScanRecord, error) {
	row := s.db.QueryRowContext(ctx, getSQL, ip, port, service)
	r := &ScanRecord{}
	err := row.Scan(&r.Ip, &r.Port, &r.Service, &r.LastScanned, &r.Response)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get scan record: %w", err)
	}
	return r, nil
}

// Ping verifies the database connection is alive by issuing a lightweight
// ping through the connection pool.
func (s *SQLiteStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// Close releases the database connection.
func (s *SQLiteStore) Close() error {
	return s.db.Close()
}
