package raft

import (
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServer_RequestVote_RejectsWhenTermIsBehindServer(t *testing.T) {
	server := NewServer(NewMemberId(1), []*ClusterMember{
		{
			Id:   NewMemberId(1),
			Addr: "",
		},
	})

	server.currentTerm = 2

	args := &RequestVoteRequest{
		Term:         1,
		CandidateID:  0,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	result := &RequestVoteResponse{}

	require.NoError(t, server.RequestVote(args, result))

	require.Equal(t, Term(2), result.Term)
	require.False(t, result.VoteGranted)
}

func TestServer_RequestVote_ReturnsFalseWhenLogIsNotUpToDate(t *testing.T) {
	server := NewServer(NewMemberId(1), []*ClusterMember{
		{
			Id:   NewMemberId(1),
			Addr: "",
		},
	})

	server.currentTerm = 1
	server.log = append(server.log, LogEntry{
		Term:    1,
		Command: []byte("test"),
	})

	args := &RequestVoteRequest{
		Term:         2,
		CandidateID:  0,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	result := &RequestVoteResponse{}

	require.NoError(t, server.RequestVote(args, result))

	require.Equal(t, Term(2), result.Term)
	require.False(t, result.VoteGranted)
}

func TestServer_RequestVote_ReturnsTrueWhenTermIsValidAndLogIsUpToDate(t *testing.T) {
	server := NewServer(NewMemberId(1), []*ClusterMember{
		{
			Id:   NewMemberId(1),
			Addr: "",
		},
	})

	server.currentTerm = 0

	args := &RequestVoteRequest{
		Term:         1,
		CandidateID:  1,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	result := &RequestVoteResponse{}

	require.NoError(t, server.RequestVote(args, result))

	assert.Equal(t, Term(1), result.Term)
	assert.True(t, result.VoteGranted)
}

func TestServer_AppendEntries_ReturnFalseIfTermLessThanCurrentTerm(t *testing.T) {
	server := NewServer(NewMemberId(1), []*ClusterMember{
		{
			Id:   NewMemberId(1),
			Addr: "",
		},
		{
			Id:   NewMemberId(2),
			Addr: "",
		},
		{
			Id:   NewMemberId(3),
			Addr: "",
		},
	})
	server.currentTerm = 2

	args := &AppendEntriesRequest{
		Term:         1,
		LeaderID:     2,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			{
				Term:    1,
				Command: []byte("test"),
			},
		},
	}

	result := &AppendEntriesResponse{
		Success: false,
		Term:    2,
	}

	require.NoError(t, server.AppendEntries(args, result))

	assert.Equal(t, Term(2), result.Term)
	assert.False(t, result.Success)
}

func TestServer_AppendEntries_ReturnFalseIfLogDoesNotContainEntryAtPrevLogIndex(t *testing.T) {
	server := NewServer(NewMemberId(1), []*ClusterMember{
		{
			Id:   NewMemberId(1),
			Addr: "",
		},
		{
			Id:   NewMemberId(2),
			Addr: "",
		},
		{
			Id:   NewMemberId(3),
			Addr: "",
		},
	})
	server.currentTerm = 2
	server.log = []LogEntry{
		{
			Term:    2,
			Command: []byte("test"),
		},
	}

	args := &AppendEntriesRequest{
		Term:         3,
		LeaderID:     2,
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries:      []LogEntry{},
	}

	result := &AppendEntriesResponse{}

	require.NoError(t, server.AppendEntries(args, result))

	require.Equal(t, Term(3), result.Term)
	require.False(t, result.Success)
}

func TestServer_AppendEntries_TransitionsToFollowerIfNewLeaderSendsRPCInCandidateState(t *testing.T) {
	server := NewServer(NewMemberId(1), []*ClusterMember{
		{
			Id:   NewMemberId(1),
			Addr: "",
		},
		{
			Id:   NewMemberId(2),
			Addr: "",
		},
		{
			Id:   NewMemberId(3),
			Addr: "",
		},
	})

	server.state = candidate
	server.currentTerm = 2

	args := &AppendEntriesRequest{
		Term:         Term(3),
		LeaderID:     NewMemberId(2),
		PrevLogIndex: 1,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
	}

	result := &AppendEntriesResponse{}

	require.NoError(t, server.AppendEntries(args, result))

	assert.Equal(t, Term(3), server.currentTerm)
	assert.Equal(t, follower, server.state)
}

func TestServer_AppendEntries_TransitionsToFollowerIfNewLeaderSendsRPCInLeaderState(t *testing.T) {
	server := NewServer(NewMemberId(1), []*ClusterMember{
		{
			Id:   NewMemberId(1),
			Addr: "",
		},
		{
			Id:   NewMemberId(2),
			Addr: "",
		},
		{
			Id:   NewMemberId(3),
			Addr: "",
		},
	})

	server.state = leader
	server.currentTerm = 2

	args := &AppendEntriesRequest{
		Term:         Term(3),
		LeaderID:     NewMemberId(2),
		PrevLogIndex: 1,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
	}

	result := &AppendEntriesResponse{}

	require.NoError(t, server.AppendEntries(args, result))

	assert.Equal(t, Term(3), server.currentTerm)
	assert.Equal(t, follower, server.state)
}

func TestServer_AppendEntries_AppendsNewEntriesToFollowers(t *testing.T) {
	server := NewServer(NewMemberId(1), []*ClusterMember{
		{
			Id:   NewMemberId(1),
			Addr: "",
		},
		{
			Id:   NewMemberId(2),
			Addr: "",
		},
		{
			Id:   NewMemberId(3),
			Addr: "",
		},
	})

	server.currentTerm = 1
	server.log = append(server.log, LogEntry{
		Term:    1,
		Command: []byte("test"),
	})

	args := &AppendEntriesRequest{
		Term:         1,
		LeaderID:     2,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			{
				Term:    1,
				Command: []byte("test2"),
			},
		},
		LeaderCommit: 1,
	}

	result := &AppendEntriesResponse{}

	require.NoError(t, server.AppendEntries(args, result))

	assert.Equal(t, Term(1), result.Term)
	assert.True(t, result.Success)
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

func TestServer_AppendEntries_AppendsNewEntriesToFollowersOverwritingInvalidEntries(t *testing.T) {
	server := NewServer(NewMemberId(1), []*ClusterMember{
		{
			Id:   NewMemberId(1),
			Addr: "",
		},
		{
			Id:   NewMemberId(2),
			Addr: "",
		},
		{
			Id:   NewMemberId(3),
			Addr: "",
		},
	})

	server.currentTerm = 1
	server.log = append(server.log, LogEntry{
		Term:    1,
		Command: []byte("test"),
	}, LogEntry{
		Term:    0,
		Command: nil,
	})

	args := &AppendEntriesRequest{
		Term:         1,
		LeaderID:     2,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			{
				Term:    1,
				Command: []byte("test2"),
			},
		},
		LeaderCommit: 1,
	}

	result := &AppendEntriesResponse{}

	require.NoError(t, server.AppendEntries(args, result))

	assert.Equal(t, Term(1), result.Term)
	assert.True(t, result.Success)
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
	server := NewServer(NewMemberId(1), []*ClusterMember{
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

	// Send a heartbeat to the follower so it knows who the leader is
	args := &AppendEntriesRequest{
		Term:         1,
		LeaderID:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}

	resp := &AppendEntriesResponse{}
	require.NoError(t, server.AppendEntries(args, resp))
	require.True(t, resp.Success)

	err := server.ApplyCommand([][]byte{
		[]byte("test"),
	})

	expectedErr := &NotLeaderError{LeaderId: 2, LeaderAddr: "two"}

	assert.Equal(t, expectedErr, err)
}

func TestServer_ApplyCommand_ReturnsErrNotLeaderWhenCandidate(t *testing.T) {
	server := NewServer(NewMemberId(1), []*ClusterMember{
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

	// Send a heartbeat to the follower so it knows who the leader is
	args := &AppendEntriesRequest{
		Term:         1,
		LeaderID:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}

	server.state = candidate

	resp := &AppendEntriesResponse{}
	require.NoError(t, server.AppendEntries(args, resp))
	require.True(t, resp.Success)

	err := server.ApplyCommand([][]byte{
		[]byte("test"),
	})

	expectedErr := &NotLeaderError{LeaderId: 2, LeaderAddr: "two"}

	assert.Equal(t, expectedErr, err)
}
