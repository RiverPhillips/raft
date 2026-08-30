package storage

import (
	"context"
	"sync"
)

type InMemoryStorage struct {
	mu       sync.Mutex
	term     uint64
	votedFor uint32
	log      []LogEntry
}

func (s *InMemoryStorage) WriteMetadata(ctx context.Context, currentTerm uint64, votedFor uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.term = currentTerm
	s.votedFor = votedFor
	return nil
}

func (s *InMemoryStorage) AppendToLog(ctx context.Context, logs ...LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = append(s.log, logs...)
	return nil
}

func (s *InMemoryStorage) LoadState(ctx context.Context) (StoredState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	log := make([]LogEntry, len(s.log))
	copy(log, s.log)
	return StoredState{CurrentTerm: s.term, VotedFor: s.votedFor, Log: log}, nil
}

// TruncateLog removes all entries from the 1-based index onwards.
func (s *InMemoryStorage) TruncateLog(ctx context.Context, idx uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// 1-based; keep entries [0, idx-1)
	keep := min(idx-1, uint64(len(s.log)))
	s.log = s.log[:keep]
	return nil
}
