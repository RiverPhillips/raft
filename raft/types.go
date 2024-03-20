package raft

import (
	"net/rpc"
)

type Term uint64

type LogEntry struct {
	Term    Term
	Command Command
}

type AppendEntriesRequest struct {
	Term         Term
	LeaderID     memberId
	PrevLogIndex uint64
	PrevLogTerm  Term
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	Term    Term
	Success bool
}

type RequestVoteRequest struct {
	Term         Term
	CandidateID  memberId
	LastLogIndex uint64
	LastLogTerm  Term
}

type RequestVoteResponse struct {
	Term        Term
	VoteGranted bool
}

type ServerState uint16

const (
	Leader ServerState = iota + 1
	Follower
	Candidate
)

type memberId uint16

func NewMemberId(id uint16) memberId {
	if id < 1 {
		panic("Member ID must be an integer greater than 0")
	}
	return memberId(id)
}

type ClusterMember struct {
	Id         memberId
	rpcClient  *rpc.Client
	Addr       string
	votedFor   memberId
	nextIndex  uint64
	matchIndex uint64
}

type NotLeaderError struct {
	LeaderId   memberId
	LeaderAddr string
}

func (e *NotLeaderError) Error() string {
	return "not the Leader"
}

type Command []byte

type Result []byte

type StateMachine interface {
	Apply(commands ...Command) []Result
}
