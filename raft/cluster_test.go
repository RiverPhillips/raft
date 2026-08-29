package raft

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type InMemoryTransport struct {
	mu      sync.RWMutex
	servers map[MemberId]*Server
	stopped map[MemberId]bool
}

func (t *InMemoryTransport) Stop(id MemberId) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stopped[id] = true
}

func (t *InMemoryTransport) AppendEntries(ctx context.Context, to MemberId, req *AppendEntriesRequest) (*AppendEntriesResult, error) {
	t.mu.RLock()
	stopped := t.stopped[to]
	server, ok := t.servers[to]
	t.mu.RUnlock()

	if !ok {
		return nil, errors.New("Unknown Member")
	}
	if stopped {
		return nil, errors.New("connection refused: member is stopped")
	}
	return server.AppendEntries(ctx, req)
}

func (t *InMemoryTransport) RequestVote(ctx context.Context, to MemberId, req *RequestVoteRequest) (*RequestVoteResult, error) {
	t.mu.RLock()
	stopped := t.stopped[to]
	server, ok := t.servers[to]
	t.mu.RUnlock()

	if !ok {
		return nil, errors.New("Unknown Member")
	}
	if stopped {
		return nil, errors.New("connection refused: member is stopped")
	}
	return server.RequestVote(ctx, req)
}

var _ Transport = (*InMemoryTransport)(nil)

type InMemoryCluster struct {
	transport *InMemoryTransport
	servers   map[MemberId]*Server
}

func (c *InMemoryCluster) Stop(id MemberId) {
	c.transport.Stop(id)
}

type RecordingStateMachine struct {
	mu       sync.Mutex
	commands []Command
}

func (r *RecordingStateMachine) Apply(cmds ...Command) []Result {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commands = append(r.commands, cmds...)
	return make([]Result, len(cmds))
}

func (r *RecordingStateMachine) Applied() []Command {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Command(nil), r.commands...)
}

func (c *InMemoryCluster) WaitLeader(t *testing.T) (MemberId, Term) {
	var leaderId MemberId
	var leaderTerm Term

	assert.Eventually(t, func() bool {
		leaderCount := 0

		for _, s := range c.servers {
			s.mu.Lock()
			state := s.state
			id := s.id
			term := s.CurrentTerm
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
		for _, s := range c.servers {
			s.mu.Lock()
			state := s.state
			term := s.CurrentTerm
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
	}, 2*time.Second, 50*time.Millisecond)

	return leaderId, leaderTerm
}

func (c *InMemoryCluster) getLogs() map[MemberId][]LogEntry {
	logs := make(map[MemberId][]LogEntry)
	for id, s := range c.servers {
		s.mu.Lock()
		// Copy the log slice under the lock
		logCopy := make([]LogEntry, len(s.Log))
		copy(logCopy, s.Log)
		logs[id] = logCopy
		s.mu.Unlock()
	}
	return logs
}

func assertLogsMatch(t *testing.T, cluster InMemoryCluster, expectedEntries ...LogEntry) {
	assert.Eventually(t, func() bool {
		logs := cluster.getLogs()
		for _, log := range logs {
			// Check length and contents (remember log[0] is sentinel)
			if len(log)-1 != len(expectedEntries) {
				return false
			}
			for i, expected := range expectedEntries {
				actual := log[i+1]
				if actual.Term != expected.Term || !bytes.Equal(actual.Command, expected.Command) {
					return false
				}
			}
		}
		return true
	}, 2*time.Second, 50*time.Millisecond)
}

func NewInMemoryCluster() InMemoryCluster {
	transport := &InMemoryTransport{
		servers: map[MemberId]*Server{},
		stopped: map[MemberId]bool{},
	}

	cluster := InMemoryCluster{
		transport: transport,
		servers:   map[MemberId]*Server{},
	}

	memberIds := []MemberId{1, 2, 3}

	for _, m := range memberIds {
		clusterMembers := []*ClusterMember{}
		for _, m := range memberIds {
			clusterMembers = append(clusterMembers, &ClusterMember{Id: m})
		}
		srv := NewServer(
			m,
			&RecordingStateMachine{},
			clusterMembers,
			transport,
		)
		transport.servers[m] = srv
		cluster.servers[m] = srv
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

	leaderId, leaderTerm := cluster.WaitLeader(t)
	assert.NotZero(t, leaderId)
	assert.NotZero(t, leaderTerm)

	canc()
	assert.NoError(t, eg.Wait())
}

func TestTheSameCommandsGetsAppliesToAllMembers(t *testing.T) {
	eg, ctx := errgroup.WithContext(t.Context())
	ctx, canc := context.WithCancel(ctx)
	cluster := NewInMemoryCluster()

	for _, s := range cluster.servers {
		// todo: this will leak
		eg.Go(func() error { return s.Start(ctx) })
	}

	leaderId, leaderTerm := cluster.WaitLeader(t)

	// Apply a command
	_, err := cluster.servers[leaderId].ApplyCommand(ctx,
		Command("test1"),
		Command("test2"),
	)
	require.NoError(t, err)

	_, err = cluster.servers[leaderId].ApplyCommand(ctx, Command("test3"))
	require.NoError(t, err)

	expectedLogs := []LogEntry{
		{
			Term:    leaderTerm,
			Command: Command("test1"),
		},
		{
			Term:    leaderTerm,
			Command: Command("test2"),
		},
		{
			Term:    leaderTerm,
			Command: Command("test3"),
		},
	}
	assertLogsMatch(t, cluster, expectedLogs...)

	assert.Eventually(t, func() bool {
		for _, s := range cluster.servers {
			sm := s.stateMachine.(*RecordingStateMachine)
			applied := sm.Applied()
			if len(applied) != 3 { // e.g. test1, test2, test3
				return false
			}
		}
		return true
	}, time.Second, time.Millisecond*50)

	canc()
	assert.NoError(t, eg.Wait())
}

func TestLeaderFailoverAndReplication(t *testing.T) {
	eg, ctx := errgroup.WithContext(t.Context())
	ctx, cancelAll := context.WithCancel(ctx)
	cluster := NewInMemoryCluster()

	serverCancels := make(map[MemberId]context.CancelFunc)

	for id, s := range cluster.servers {
		srvCtx, srvCancel := context.WithCancel(ctx)
		serverCancels[id] = srvCancel
		eg.Go(func() error { return s.Start(srvCtx) })
	}

	// 1. Wait for initial leader
	leader1, term1 := cluster.WaitLeader(t)

	// 2. Apply a command on initial leader
	_, err := cluster.servers[leader1].ApplyCommand(ctx, Command("cmd1"))
	require.NoError(t, err)

	// 3. Stop the initial leader
	cluster.Stop(leader1)
	serverCancels[leader1]()

	// 4. Wait for a new leader to emerge among the remaining 2 servers
	var newLeader MemberId
	var newTerm Term
	assert.Eventually(t, func() bool {
		for id, s := range cluster.servers {
			if id == leader1 {
				continue
			}
			s.mu.Lock()
			state := s.state
			term := s.CurrentTerm
			s.mu.Unlock()

			if state == Leader && term > term1 {
				newLeader = id
				newTerm = term
				return true
			}
		}
		return false
	}, 3*time.Second, 50*time.Millisecond)

	require.NotEqual(t, leader1, newLeader)
	require.Greater(t, newTerm, term1)

	// 5. Apply a new command to the new leader
	_, err = cluster.servers[newLeader].ApplyCommand(ctx, Command("cmd2"))
	require.NoError(t, err)

	// 6. Assert both surviving nodes have both cmd1 and cmd2
	assert.Eventually(t, func() bool {
		logs := cluster.getLogs()
		for id, log := range logs {
			if id == leader1 {
				continue // Skip stopped node
			}
			if len(log)-1 != 2 {
				return false
			}
			if !bytes.Equal(log[1].Command, Command("cmd1")) || log[1].Term != term1 {
				return false
			}
			if !bytes.Equal(log[2].Command, Command("cmd2")) || log[2].Term != newTerm {
				return false
			}
		}
		return true
	}, 2*time.Second, 50*time.Millisecond)

	cancelAll()
	assert.NoError(t, eg.Wait())
}

func TestLeaderDoesNotCommitWithoutQuorum(t *testing.T) {

	eg, ctx := errgroup.WithContext(t.Context())
	ctx, canc := context.WithCancel(ctx)
	cluster := NewInMemoryCluster()

	for _, s := range cluster.servers {
		// todo: this will leak
		eg.Go(func() error { return s.Start(ctx) })
	}

	leaderId, _ := cluster.WaitLeader(t)
	leader := cluster.servers[leaderId]

	// Read initial commit index
	leader.mu.Lock()
	initialCommit := leader.commitIndex
	leader.mu.Unlock()

	// Isolate the leader by stopping all other members
	for id := range cluster.servers {
		if id != leaderId {
			cluster.Stop(id)
		}
	}

	// Attempt to apply a command with a short timeout
	timeoutCtx, timeoutCanc := context.WithTimeout(ctx, 100*time.Millisecond)
	defer timeoutCanc()

	cmd := Command("uncommitted-command")
	_, err := leader.ApplyCommand(timeoutCtx, cmd)
	require.Error(t, err, "ApplyCommand should fail when quorum is unreachable")

	// Verify leader did NOT advance commitIndex or apply to state machine
	leader.mu.Lock()
	assert.Equal(t, initialCommit, leader.commitIndex, "Leader commitIndex must not advance without quorum acknowledgment")
	leader.mu.Unlock()

	applied := (leader.stateMachine.(*RecordingStateMachine)).Applied() // using RecordingStateMachine
	assert.NotContains(t, applied, cmd, "Uncommitted command must not be applied to StateMachine")

	canc()
	assert.NoError(t, eg.Wait())
}
