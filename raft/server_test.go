package raft

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

type noopTransport struct{}

func (noopTransport) RequestVote(context.Context, MemberId, *RequestVoteRequest) (*RequestVoteResult, error) {
	return nil, nil
}
func (noopTransport) AppendEntries(context.Context, MemberId, *AppendEntriesRequest) (*AppendEntriesResult, error) {
	return nil, nil
}

func createNewServer() *Server {
	sm := &NoOpStateMachine{}
	return NewServer(NewMemberId(1), sm, []*ClusterMember{
		{
			Id: NewMemberId(1),
		},
		{
			Id: NewMemberId(2),
		},
		{
			Id: NewMemberId(3),
		},
	}, noopTransport{})
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestServer_RequestVote_RejectsWhenTermIsBehindServer(t *testing.T) {
	server := createNewServer()

	server.currentTerm = 2

	req := &RequestVoteRequest{
		Term:         1,
		CandidateId:  0,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	res, err := server.RequestVote(context.Background(), req)
	require.NoError(t, err)

	require.Equal(t, Term(2), res.Term)
	require.False(t, res.VoteGranted)
}

func TestServer_RequestVote_ReturnsFalseWhenLogIsNotUpToDate(t *testing.T) {
	server := createNewServer()

	server.currentTerm = 1
	server.log = append(server.log, LogEntry{
		Term:    1,
		Command: []byte("test"),
	})

	req := &RequestVoteRequest{
		Term:         2,
		CandidateId:  0,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	res, err := server.RequestVote(context.Background(), req)
	require.NoError(t, err)

	require.Equal(t, Term(2), res.Term)
	require.False(t, res.VoteGranted)
}

func TestServer_RequestVote_ReturnsTrueWhenTermIsValidAndLogIsUpToDate(t *testing.T) {
	server := createNewServer()

	server.currentTerm = 0

	req := &RequestVoteRequest{
		Term:         1,
		CandidateId:  1,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	res, err := server.RequestVote(context.Background(), req)
	require.NoError(t, err)

	assert.Equal(t, Term(1), res.Term)
	assert.True(t, res.VoteGranted)
}

func TestServer_AppendEntries_ReturnFalseIfTermLessThanCurrentTerm(t *testing.T) {
	server := createNewServer()
	server.currentTerm = 2

	req := &AppendEntriesRequest{
		Term:         1,
		LeaderId:     2,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			{
				Term:    1,
				Command: []byte("test"),
			},
		},
	}

	res, err := server.AppendEntries(context.Background(), req)
	require.NoError(t, err)

	assert.Equal(t, Term(2), res.Term)
	assert.False(t, res.Success)
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

	req := &AppendEntriesRequest{
		Term:         3,
		LeaderId:     2,
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries:      []LogEntry{},
	}

	res, err := server.AppendEntries(context.Background(), req)
	require.NoError(t, err)

	require.Equal(t, Term(3), res.Term)
	require.False(t, res.Success)
}

func TestServer_AppendEntries_TransitionsToFollowerIfNewLeaderSendsRPCInCandidateState(t *testing.T) {
	server := createNewServer()

	server.state = Candidate
	server.currentTerm = 2

	req := &AppendEntriesRequest{
		Term:         3,
		LeaderId:     2,
		PrevLogIndex: 1,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
	}

	_, err := server.AppendEntries(context.Background(), req)
	require.NoError(t, err)

	assert.Equal(t, Term(3), server.currentTerm)
	assert.Equal(t, Follower, server.state)
}

func TestServer_AppendEntries_TransitionsToFollowerIfNewLeaderSendsRPCInLeaderState(t *testing.T) {
	server := createNewServer()

	server.state = Leader
	server.currentTerm = 2

	req := (&AppendEntriesRequest{
		Term:         3,
		LeaderId:     2,
		PrevLogIndex: 1,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
	})

	_, err := server.AppendEntries(context.Background(), req)
	require.NoError(t, err)

	assert.Equal(t, Term(3), server.currentTerm)
	assert.Equal(t, Follower, server.state)
}

func TestServer_AppendEntries_AppendsNewEntriesToFollowers(t *testing.T) {
	server := createNewServer()

	server.currentTerm = 1

	args := &AppendEntriesRequest{
		Term:         1,
		LeaderId:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{
				Term:    1,
				Command: []byte("test"),
			},
		},
		LeaderCommit: 1,
	}

	req := (args)
	res, err := server.AppendEntries(context.Background(), req)
	require.NoError(t, err)

	assert.Equal(t, Term(1), res.Term)
	assert.True(t, res.Success)
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

	args := &AppendEntriesRequest{
		Term:         1,
		LeaderId:     2,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			{
				Term:    1,
				Command: []byte("test2"),
			},
		},
		LeaderCommit: 2,
	}

	req := (args)
	res, err := server.AppendEntries(context.Background(), req)
	require.NoError(t, err)

	assert.Equal(t, Term(1), res.Term)
	assert.True(t, res.Success)
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
	req := (&AppendEntriesRequest{
		Term:         1,
		LeaderId:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	})
	res, err := server.AppendEntries(context.Background(), req)
	require.NoError(t, err)

	require.True(t, res.Success)

	result, err := server.ApplyCommand(context.Background(), []byte("test"))
	assert.Nil(t, result)

	expectedErr := &NotLeaderError{LeaderId: 2}

	assert.Equal(t, expectedErr, err)
}

func TestServer_ApplyCommand_ReturnsErrNotLeaderWhenCandidate(t *testing.T) {
	server := createNewServer()

	// Send a heartbeat to the Follower so it knows who the Leader is

	server.state = Candidate
	req := (&AppendEntriesRequest{
		Term:         1,
		LeaderId:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	})

	res, err := server.AppendEntries(context.Background(), req)
	require.NoError(t, err)

	require.True(t, res.Success)

	resp, err := server.ApplyCommand(context.Background(), []byte("test"))
	assert.Nil(t, resp)

	expectedErr := &NotLeaderError{LeaderId: 2}

	assert.Equal(t, expectedErr, err)
}
