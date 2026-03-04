package store

import (
	"context"
	"fmt"
	"sync"
)

// MemoryStore is an in-memory Store implementation backed by a plain map.
// It is useful for testing and local development where persistence across
// restarts is not required. It is safe for concurrent use.
type MemoryStore struct {
	mu      sync.RWMutex
	records map[string]ScanRecord
}

// NewMemoryStore returns an initialised, empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{records: make(map[string]ScanRecord)}
}

func (m *MemoryStore) key(ip string, port uint32, service string) string {
	return fmt.Sprintf("%s:%d:%s", ip, port, service)
}

// Upsert inserts or conditionally updates a scan record, applying the same
// "keep newest" semantics as SQLiteStore: the record is replaced only when
// the incoming timestamp is strictly greater than the stored one.
func (m *MemoryStore) Upsert(_ context.Context, r ScanRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := m.key(r.Ip, r.Port, r.Service)
	if existing, ok := m.records[k]; !ok || r.LastScanned > existing.LastScanned {
		m.records[k] = r
	}
	return nil
}

// Get retrieves the stored record for the given (ip, port, service) key.
// Returns nil, nil if no record exists.
func (m *MemoryStore) Get(_ context.Context, ip string, port uint32, service string) (*ScanRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.records[m.key(ip, port, service)]
	if !ok {
		return nil, nil
	}
	// Return a copy so callers cannot mutate internal state.
	c := r
	return &c, nil
}

// Close is a no-op for MemoryStore; it exists to satisfy the Store interface.
func (m *MemoryStore) Close() error { return nil }
