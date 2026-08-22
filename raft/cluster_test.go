package raft

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

type InMemoryTransport struct {
	servers map[MemberId]*Server
}

func (t *InMemoryTransport) AppendEntries(ctx context.Context, to MemberId, req *AppendEntriesRequest) (*AppendEntriesResult, error) {
	server, ok := t.servers[to]
	if !ok {
		return nil, errors.New("Unknown Member")
	}
	return server.AppendEntries(ctx, req)
}

func (t *InMemoryTransport) RequestVote(ctx context.Context, to MemberId, req *RequestVoteRequest) (*RequestVoteResult, error) {
	server, ok := t.servers[to]
	if !ok {
		return nil, errors.New("Unknown Member")
	}
	return server.RequestVote(ctx, req)
}

var _ Transport = (*InMemoryTransport)(nil)

type InMemoryCluster struct {
	transport Transport
	servers   []*Server
}

func NewInMemoryCluster() InMemoryCluster {
	transport := &InMemoryTransport{
		servers: map[MemberId]*Server{},
	}

	cluster := InMemoryCluster{
		transport: transport,
		servers:   []*Server{},
	}

	memberIds := []MemberId{1, 2, 3}

	for _, m := range memberIds {
		clusterMembers := []*ClusterMember{}
		for _, m := range memberIds {
			clusterMembers = append(clusterMembers, &ClusterMember{Id: m})
		}
		srv := NewServer(
			m,
			&NoOpStateMachine{},
			clusterMembers,
			transport,
		)
		transport.servers[m] = srv
		cluster.servers = append(cluster.servers, srv)
	}

	return cluster
}

func TestElectsALeader(t *testing.T) {

	eg, ctx := errgroup.WithContext(t.Context())
	ctx, canc := context.WithCancel(ctx)
	cluster := NewInMemoryCluster()

	for _, s := range cluster.servers {
		// todo: this will leak
		eg.Go(func() error { return s.Start(ctx) })
	}

	assert.Eventually(t, func() bool {
		var leaderId MemberId
		var leaderTerm Term
		leaderCount := 0

		for _, s := range cluster.servers {
			s.mu.Lock()
			state := s.state
			id := s.id
			term := s.currentTerm
			s.mu.Unlock()

			if state == Leader {
				leaderCount++
				leaderId = id
				leaderTerm = term
			}
		}

		if leaderCount != 1 || leaderTerm == 0 {
			return false
		}

		// Ensure all followers agree on the term and know the leader
		for _, s := range cluster.servers {
			s.mu.Lock()
			state := s.state
			term := s.currentTerm
			leader := s.leader
			s.mu.Unlock()

			if term != leaderTerm {
				return false
			}

			if state == Follower {
				if leader == nil || leader.Id != leaderId {
					return false
				}
			}
		}

		return true
	}, 2*time.Second, time.Millisecond*50)
	canc()
	assert.NoError(t, eg.Wait())
}
