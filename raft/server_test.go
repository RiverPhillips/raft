package raft

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	v1 "github.com/RiverPhillips/raft/gen/proto/raft/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServer_RequestVote_RejectsWhenTermIsBehindServer(t *testing.T) {
	server := createNewServer()

	server.currentTerm = 2

	req := connect.NewRequest(&v1.RequestVoteRequest{
		Term:         1,
		CandidateId:  0,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	res, err := server.RequestVote(context.Background(), req)
	require.NoError(t, err)

	msg := res.Msg

	require.Equal(t, uint64(2), msg.Term)
	require.False(t, msg.VoteGranted)
}

func createNewServer() *Server {
	sm := &NoOpStateMachine{}
	return NewServer(NewMemberId(1), sm, []*ClusterMember{
		{
			Id:   NewMemberId(1),
			Addr: "one",
		},
		{
			Id:   NewMemberId(2),
			Addr: "two",
		},
		{
			Id:   NewMemberId(3),
			Addr: "three",
		},
	})
}

func TestServer_RequestVote_ReturnsFalseWhenLogIsNotUpToDate(t *testing.T) {
	server := createNewServer()

	server.currentTerm = 1
	server.log = append(server.log, LogEntry{
		Term:    1,
		Command: []byte("test"),
	})

	req := connect.NewRequest(&v1.RequestVoteRequest{
		Term:         2,
		CandidateId:  0,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	res, err := server.RequestVote(context.Background(), req)
	require.NoError(t, err)

	msg := res.Msg

	require.Equal(t, uint64(2), msg.Term)
	require.False(t, msg.VoteGranted)
}

func TestServer_RequestVote_ReturnsTrueWhenTermIsValidAndLogIsUpToDate(t *testing.T) {
	server := createNewServer()

	server.currentTerm = 0

	req := connect.NewRequest(&v1.RequestVoteRequest{
		Term:         1,
		CandidateId:  1,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	res, err := server.RequestVote(context.Background(), req)
	require.NoError(t, err)

	msg := res.Msg

	assert.Equal(t, uint64(1), msg.Term)
	assert.True(t, msg.VoteGranted)
}

func TestServer_AppendEntries_ReturnFalseIfTermLessThanCurrentTerm(t *testing.T) {
	server := createNewServer()
	server.currentTerm = 2

	args := &v1.AppendEntriesRequest{
		Term:         1,
		LeaderId:     2,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []*v1.LogEntry{
			{
				Term:    1,
				Command: []byte("test"),
			},
		},
	}

	req := connect.NewRequest(args)
	res, err := server.AppendEntries(context.Background(), req)
	require.NoError(t, err)

	msg := res.Msg
	assert.Equal(t, uint64(2), msg.Term)
	assert.False(t, msg.Success)
}

func TestServer_AppendEntries_ReturnFalseIfLogDoesNotContainEntryAtPrevLogIndex(t *testing.T) {
	server := createNewServer()
	server.currentTerm = 2
	server.log = []LogEntry{
		{
			Term:    2,
			Command: []byte("test"),
		},
	}

	args := &v1.AppendEntriesRequest{
		Term:         3,
		LeaderId:     2,
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries:      []*v1.LogEntry{},
	}

	req := connect.NewRequest(args)
	res, err := server.AppendEntries(context.Background(), req)
	require.NoError(t, err)

	msg := res.Msg
	require.Equal(t, uint64(3), msg.Term)
	require.False(t, msg.Success)
}

func TestServer_AppendEntries_TransitionsToFollowerIfNewLeaderSendsRPCInCandidateState(t *testing.T) {
	server := createNewServer()

	server.state = Candidate
	server.currentTerm = 2

	args := &v1.AppendEntriesRequest{
		Term:         3,
		LeaderId:     2,
		PrevLogIndex: 1,
		PrevLogTerm:  0,
		Entries:      []*v1.LogEntry{},
	}

	req := connect.NewRequest(args)
	_, err := server.AppendEntries(context.Background(), req)
	require.NoError(t, err)

	assert.Equal(t, Term(3), server.currentTerm)
	assert.Equal(t, Follower, server.state)
}

func TestServer_AppendEntries_TransitionsToFollowerIfNewLeaderSendsRPCInLeaderState(t *testing.T) {
	server := createNewServer()

	server.state = Leader
	server.currentTerm = 2

	req := connect.NewRequest(&v1.AppendEntriesRequest{
		Term:         3,
		LeaderId:     2,
		PrevLogIndex: 1,
		PrevLogTerm:  0,
		Entries:      []*v1.LogEntry{},
	})

	_, err := server.AppendEntries(context.Background(), req)
	require.NoError(t, err)

	assert.Equal(t, Term(3), server.currentTerm)
	assert.Equal(t, Follower, server.state)
}

func TestServer_AppendEntries_AppendsNewEntriesToFollowers(t *testing.T) {
	server := createNewServer()

	server.currentTerm = 1

	args := &v1.AppendEntriesRequest{
		Term:         1,
		LeaderId:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []*v1.LogEntry{
			{
				Term:    1,
				Command: []byte("test"),
			},
		},
		LeaderCommit: 1,
	}

	req := connect.NewRequest(args)
	res, err := server.AppendEntries(context.Background(), req)
	require.NoError(t, err)

	msg := res.Msg

	assert.Equal(t, uint64(1), msg.Term)
	assert.True(t, msg.Success)
	assert.Equal(t, []LogEntry{
		{
			Term:    0,
			Command: nil,
		},
		{
			Term:    1,
			Command: []byte("test"),
		},
	}, server.log)
}

func TestServer_AppendEntries_AppendsNewEntriesToFollowersOverwritingInvalidEntries(t *testing.T) {
	server := createNewServer()

	server.currentTerm = 1
	server.log = append(server.log, LogEntry{
		Term:    1,
		Command: []byte("test"),
	}, LogEntry{
		Term:    3,
		Command: nil,
	})

	args := &v1.AppendEntriesRequest{
		Term:         1,
		LeaderId:     2,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []*v1.LogEntry{
			{
				Term:    1,
				Command: []byte("test2"),
			},
		},
		LeaderCommit: 2,
	}

	req := connect.NewRequest(args)
	res, err := server.AppendEntries(context.Background(), req)
	require.NoError(t, err)

	msg := res.Msg

	assert.Equal(t, uint64(1), msg.Term)
	assert.True(t, msg.Success)
	assert.Equal(t, []LogEntry{
		{
			Term:    0,
			Command: nil,
		},
		{
			Term:    1,
			Command: []byte("test"),
		},
		{
			Term:    1,
			Command: []byte("test2"),
		},
	}, server.log)
}

func TestServer_ApplyCommand_ReturnsErrNotLeaderWhenFollower(t *testing.T) {
	server := createNewServer()

	// Send a heartbeat to the Follower so it knows who the Leader is
	req := connect.NewRequest(&v1.AppendEntriesRequest{
		Term:         1,
		LeaderId:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []*v1.LogEntry{},
		LeaderCommit: 0,
	})
	res, err := server.AppendEntries(context.Background(), req)
	require.NoError(t, err)

	msg := res.Msg
	require.True(t, msg.Success)

	result, err := server.ApplyCommand([]byte("test"))
	assert.Nil(t, result)

	expectedErr := &NotLeaderError{LeaderId: 2, LeaderAddr: "two"}

	assert.Equal(t, expectedErr, err)
}

func TestServer_ApplyCommand_ReturnsErrNotLeaderWhenCandidate(t *testing.T) {
	server := createNewServer()

	// Send a heartbeat to the Follower so it knows who the Leader is

	server.state = Candidate
	req := connect.NewRequest(&v1.AppendEntriesRequest{
		Term:         1,
		LeaderId:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []*v1.LogEntry{},
		LeaderCommit: 0,
	})

	res, err := server.AppendEntries(context.Background(), req)
	require.NoError(t, err)

	msg := res.Msg
	require.True(t, msg.Success)

	resp, err := server.ApplyCommand([]byte("test"))
	assert.Nil(t, resp)

	expectedErr := &NotLeaderError{LeaderId: 2, LeaderAddr: "two"}

	assert.Equal(t, expectedErr, err)
}
