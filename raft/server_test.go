package raft

import (
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServer_AppendEntries_ReturnFalseIfTermLessThanCurrentTerm(t *testing.T) {
	server := NewServer(NewMemberId(1), []*ClusterMember{
		{
			Id:   NewMemberId(1),
			Addr: "",
		},
	})
	server.currentTerm = 2

	args := &AppendEntriesRequest{
		Term:         1,
		LeaderID:     0,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{
				Term:    0,
				Command: []byte("test"),
			},
		},
	}

	result := &AppendEntriesResponse{
		Success: false,
		Term:    2,
	}

	require.NoError(t, server.AppendEntries(args, result))

	require.Equal(t, 2, result.Term)
	require.False(t, result.Success)
}

func TestServer_AppendEntries_ReturnFalseIfLogDoesNotContainEntryAtPrevLogIndex(t *testing.T) {
	server := NewServer(NewMemberId(1), []*ClusterMember{
		{
			Id:   NewMemberId(1),
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
		LeaderID:     0,
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries:      []LogEntry{},
	}

	result := &AppendEntriesResponse{}

	require.NoError(t, server.AppendEntries(args, result))

	require.Equal(t, 2, result.Term)
	require.False(t, result.Success)
}

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
