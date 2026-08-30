package storage

type StoredState struct {
	// Persistent state on all servers
	CurrentTerm uint64
	VotedFor    uint32
	Log         []LogEntry
}
