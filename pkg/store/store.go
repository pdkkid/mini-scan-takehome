package store

import "context"

// ScanRecord holds the latest observed state of a unique (IP, port, service) tuple.
type ScanRecord struct {
	IP          string
	Port        uint32
	Service     string
	LastScanned int64  // Unix timestamp of the most recent scan seen
	Response    string // Decoded service response string
}

// Store is the persistence abstraction for scan records.
// Implementations must be safe for concurrent use.
type Store interface {
	// Upsert inserts a new ScanRecord or updates the existing one for the
	// (ip, port, service) key ONLY IF the incoming timestamp is strictly
	// greater than the stored timestamp. This makes the operation idempotent
	// and safe under at-least-once delivery and out-of-order messages.
	Upsert(ctx context.Context, record ScanRecord) error

	// Get retrieves the current record for a (ip, port, service) key.
	// Returns nil, nil if no record exists for that key.
	Get(ctx context.Context, ip string, port uint32, service string) (*ScanRecord, error)

	// Ping verifies the store is reachable and usable. It is called by the
	// health-check endpoint and should return quickly.
	Ping(ctx context.Context) error

	// Close releases any resources held by the store.
	Close() error
}
