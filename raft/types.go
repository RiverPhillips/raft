package raft

import (
	"context"
)

type Term uint64

type LogEntry struct {
	Term    Term
	Command Command
}

type ServerState uint16

const (
	Leader ServerState = iota + 1
	Follower
	Candidate
)

type MemberId uint32

func NewMemberId(id uint32) MemberId {
	if id < 1 {
		panic("Member ID must be an integer greater than 0")
	}
	return MemberId(id)
}

type ClusterMember struct {
	Id         MemberId
	votedFor   MemberId
	nextIndex  uint64
	matchIndex uint64
}

type NotLeaderError struct {
	LeaderId MemberId
}

func (e *NotLeaderError) Error() string {
	return "not the Leader"
}

type Command []byte

type Result []byte

type StateMachine interface {
	Apply(commands ...Command) []Result
}

// Invoked by leader to replicate log entries; also used as heartbeat
type AppendEntriesRequest struct {
	// leader's term
	Term Term
	// so followers can redirect clients
	LeaderId MemberId
	// index of log entry immediately preceding new ones
	PrevLogIndex uint64
	// term of prevLogEntry
	PrevLogTerm Term
	//log entries to store (empty for heartbeat; may send more than one for efficiency)
	Entries []*LogEntry
	// leader's commit index
	LeaderCommit uint64
}

// Result of [AppendEntriesRequest/
type AppendEntriesResult struct {
	//currentTerm, for leader to update itself
	Term Term
	// true if follower contained entry matching prevLogIndex and prevLogTerm
	Success bool
}

// Invoked by candidates to gather votes
type RequestVoteRequest struct {
	// Candidate's term
	Term Term
	// Candidate requesting the vote
	CandidateId MemberId
	// index of candidate's last log entry
	LastLogIndex uint64
	// term of canidate's last log entry
	LastLogTerm Term
}

// Result of [RequestVoteRequest]
type RequestVoteResult struct {
	//currentTerm, for candidate to update itself
	Term Term
	// true means candidate received vote
	VoteGranted bool
}

type Transport interface {
	RequestVote(ctx context.Context, to MemberId, req *RequestVoteRequest) (*RequestVoteResult, error)
	AppendEntries(ctx context.Context, to MemberId, req *AppendEntriesRequest) (*AppendEntriesResult, error)
}
