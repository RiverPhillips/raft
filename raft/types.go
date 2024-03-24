package raft

import (
	"github.com/RiverPhillips/raft/gen/proto/raft/v1/raftv1connect"
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
	rpcClient  raftv1connect.RaftServiceClient
	Addr       string
	votedFor   MemberId
	nextIndex  uint64
	matchIndex uint64
}

type NotLeaderError struct {
	LeaderId   MemberId
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
