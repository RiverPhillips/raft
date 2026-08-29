// Package raft server implementation
package raft

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

const (
	heartbeatTimeout   = time.Millisecond * 50
	minElectionTimeout = 150
)

type volatileState struct {
	commitIndex uint64
	lastApplied uint64
}

type Server struct {
	mu              sync.Mutex
	id              MemberId
	electionTicker  *time.Ticker
	heartbeatTicker *time.Ticker

	clusterMembers map[MemberId]*ClusterMember
	transport      Transport

	PersistentState
	volatileState

	storage Storage

	state ServerState

	leader *ClusterMember

	stateMachine StateMachine
}

func getElectionTimeout() time.Duration {
	return time.Millisecond * time.Duration(minElectionTimeout+rand.IntN(150))
}

func NewServer(id MemberId, sm StateMachine, members []*ClusterMember, transport Transport, storage Storage, opts ...func(*Server) *Server) *Server {
	if id < 1 {
		panic("Server ID must be an integer greater than 0")
	}

	if len(members)%2 == 0 {
		panic("Cluster must have an odd number of members")
	}

	if transport == nil {
		panic("Transport is required")
	}

	if storage == nil {
		panic("Storage is required")
	}

	hbTicker := time.NewTicker(heartbeatTimeout)
	hbTicker.Stop()

	mm := map[MemberId]*ClusterMember{}

	for _, m := range members {
		mm[m.Id] = m
	}

	s := &Server{
		state:           Follower,
		id:              id,
		electionTicker:  time.NewTicker(getElectionTimeout()),
		heartbeatTicker: hbTicker,
		clusterMembers:  mm,
		PersistentState: PersistentState{
			Log: []LogEntry{{Term: 0}},
		},
		stateMachine: sm,
		transport:    transport,
		storage:      storage,
	}

	for _, opt := range opts {
		s = opt(s)
	}
	return s
}

// AppendEntries is an RPC method that is called by the Leader to replicate log entries
func (s *Server) AppendEntries(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesResult, error) {
	// 1. Reply false if Term < currentTerm
	s.mu.Lock()
	defer s.mu.Unlock()
	if req.Term < s.CurrentTerm {
		return &AppendEntriesResult{
			Term:    s.CurrentTerm,
			Success: false,
		}, nil
	}

	if s.state != Follower && req.Term >= s.CurrentTerm {
		s.heartbeatTicker.Stop()
		s.state = Follower
		s.resetElectionTimer()
	}

	if err := s.updateTerm(ctx, req.Term); err != nil {
		return nil, err
	}

	resp := &AppendEntriesResult{}

	if s.state != Follower {
		panic("Only followers should be receiving append entries")
	}

	leaderId := req.LeaderId

	if s.leader == nil || leaderId != s.leader.Id {
		s.leader = s.getMemberById(leaderId)
	}

	resp.Term = s.CurrentTerm
	resp.Success = false

	// Reply false if term < currentTerm
	if Term(req.Term) < s.CurrentTerm {
		// This is a stale request from an old Leader
		slog.Debug("Rejecting append entries request from stale Leader", "server", s.id, "term", req.Term, "currentTerm", s.CurrentTerm, "Leader", req.LeaderId)
		return resp, nil
	}

	// We have a valid Leader
	s.resetElectionTimer()

	prevLogTerm := Term(req.PrevLogTerm)
	slog.Debug("Received append entries request from valid Leader", "server", s.id, "term", req.Term, "Leader", req.LeaderId, "prevLogIndex", req.PrevLogIndex, "prevLogTerm", prevLogTerm, "entries", len(req.Entries))

	// 2. Reply false if log does not contain an entry at prevLogIndex whose Term matches prevLogTerm
	logLen := uint64(len(s.Log))
	validLog := req.PrevLogIndex < logLen && s.Log[req.PrevLogIndex].Term == prevLogTerm

	if !validLog {
		slog.Debug("Rejecting append entries request. Log was not valid", "server", s.id, "term", req.Term, "Leader", req.LeaderId, "prevLogIndex", req.PrevLogIndex, "prevLogTerm", prevLogTerm, "logLength", logLen)
		return (resp), nil
	}

	nextIdx := req.PrevLogIndex + 1

	var newEntries []LogEntry
	for idx, entry := range req.Entries {
		entryIdx := nextIdx + uint64(idx)
		ent := LogEntry{Term: Term(entry.Term), Command: entry.Command}

		if entryIdx < uint64(len(s.Log)) {
			if s.Log[entryIdx].Term != ent.Term {
				// Conflict found! Truncate in-memory and on-disk from entryIdx onwards
				s.Log = s.Log[:entryIdx]
				if err := s.storage.TruncateLog(ctx, entryIdx); err != nil {
					if errors.Is(err, context.Canceled) || ctx.Err() != nil {
						return nil, ctx.Err()
					}
					slog.Error("Failed to truncate log", "error", err)
					panic("failed to truncate log")
				}
				// Append the replacement entry
				s.Log = append(s.Log, ent)
				newEntries = append(newEntries, ent)
			}
			// If terms match, the entry is already identical: do nothing and continue
		} else {
			// Entry is beyond the current log: append it
			s.Log = append(s.Log, ent)
			newEntries = append(newEntries, ent)
		}
	}

	if err := s.storage.AppendToLog(ctx, newEntries...); err != nil {
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			return nil, ctx.Err()
		}
		slog.Error("Failed to append to log.", "error", err)
		panic("Failed to append to log.")
	}

	if req.LeaderCommit > s.commitIndex {
		prevCommitIdx := s.commitIndex
		s.commitIndex = min(req.LeaderCommit, uint64(len(s.Log)-1))
		for i := prevCommitIdx + 1; i <= s.commitIndex; i++ {
			s.stateMachine.Apply(s.Log[i].Command)
		}
	}

	resp.Success = true

	return resp, nil
}

// Must be called with the lock held
func (s *Server) updateTerm(ctx context.Context, term Term) error {
	if term > s.CurrentTerm {
		s.heartbeatTicker.Stop()
		s.state = Follower
		s.CurrentTerm = term
		s.VotedFor = 0
		err := s.storage.WriteMetadata(ctx, s.CurrentTerm, s.VotedFor)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return err
			}
			slog.Error("Failed to update metadata on disk.", "error", err)
			panic("Failed to update metadata")

		}
		s.resetElectionTimer()
		return nil
	}
	return nil
}

// Must be called with the lock held
func (s *Server) resetElectionTimer() {
	s.electionTicker.Reset(getElectionTimeout())
}

// RequestVote is an RPC method that is called by candidates to gather votes
func (s *Server) RequestVote(ctx context.Context, req *RequestVoteRequest) (*RequestVoteResult, error) {
	reqTerm := Term(req.Term)
	candidateId := MemberId(req.CandidateId)
	slog.Info("Received request for vote", "server", s.id, "term", reqTerm, "Candidate", candidateId)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.updateTerm(ctx, reqTerm); err != nil {
		return nil, err
	}

	resp := &RequestVoteResult{}

	resp.Term = s.CurrentTerm
	resp.VoteGranted = false

	if reqTerm < s.CurrentTerm {
		slog.Debug("Rejecting vote request. Term not valid", "server", s.id, "term", reqTerm, "Candidate", candidateId, "currentTerm", s.CurrentTerm)
		return (resp), nil
	}

	logLen := uint64(len(s.Log) - 1)
	myLastTerm := s.Log[logLen].Term
	reqLastTerm := Term(req.LastLogTerm)

	logValid := reqLastTerm > myLastTerm || (reqLastTerm == myLastTerm && req.LastLogIndex >= logLen)
	grantVote := reqTerm >= s.CurrentTerm && (s.VotedFor == 0 || s.VotedFor == candidateId) && logValid

	if grantVote {
		slog.Debug("Voting for server", "server", candidateId)
		s.VotedFor = candidateId
		if err := s.storage.WriteMetadata(ctx, s.CurrentTerm, s.VotedFor); err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return nil, err
			}
			slog.Error("Failed to persist vote metadata on disk", "error", err)
			panic("Failed to persist vote metadata")
		}
		resp.VoteGranted = true
		s.resetElectionTimer()
		return (resp), nil
	} else {
		slog.Debug("Rejecting vote request. Log was not up to date enough", "server", s.id, "term", reqTerm, "Candidate", req.CandidateId, "votedFor", s.VotedFor, "lastLogIndex", req.LastLogIndex, "logLength", logLen)
	}
	return (resp), nil
}

func (s *Server) Start(ctx context.Context) error {
	// todo: Load state from disk
	state, err := s.storage.LoadState(ctx)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.CurrentTerm = state.CurrentTerm
	s.VotedFor = state.VotedFor
	s.Log = append([]LogEntry{{Term: 0, Command: nil}}, state.Log...)
	s.mu.Unlock()

	slog.Debug("Starting server as Follower")

	// Start the election timer
	// If the election timer elapses without receiving AppendEntries RPC from the current Leader or granting a vote to another Candidate, convert to Candidate
	for {
		select {
		case <-ctx.Done():
			slog.Debug("Shutting down raft server")
			return nil
		case <-s.electionTicker.C:
			slog.Debug("Election timer elapsed, transitioning to Candidate")
			s.mu.Lock()
			if s.state == Leader {
				s.mu.Unlock()
				panic("Illegal state transition from Leader to Candidate")
			}
			s.state = Candidate
			s.CurrentTerm++
			s.VotedFor = s.id
			if err := s.storage.WriteMetadata(ctx, s.CurrentTerm, s.VotedFor); err != nil {
				if errors.Is(err, context.Canceled) || ctx.Err() != nil {
					s.mu.Unlock()
					return nil
				}
				slog.Error("Failed to persist election metadata on disk", "error", err)
				panic("Failed to persist election metadata")
			}

			var wg sync.WaitGroup
			wg.Add(len(s.clusterMembers))
			for _, member := range s.clusterMembers {
				member.votedFor = 0
				if member.Id == s.id {
					// Vote for ourselves, we don't need to send a request to ourselves
					member.votedFor = s.id
					s.resetElectionTimer()
					continue
				}

				go s.requestVoteFromMember(ctx, member)
			}
			s.checkIfElected(ctx)
			s.mu.Unlock()
		case <-s.heartbeatTicker.C:
			s.mu.Lock()
			if s.state != Leader {
				panic("Only leaders should be sending heartbeats")
			}
			s.mu.Unlock()
			slog.Debug("Sending heartbeat")
			s.sendHeartbeat(context.TODO())
		}
	}
}

func (s *Server) ApplyCommand(ctx context.Context, cmds ...Command) ([]Result, error) {
	slog.Debug("Received command", "commands", len(cmds))
	s.mu.Lock()

	if s.state != Leader {
		var leaderId MemberId
		if s.leader != nil {
			leaderId = s.leader.Id
		}
		s.mu.Unlock()
		return nil, &NotLeaderError{LeaderId: leaderId}
	}

	// Reject empty or oversized commands — sentinel {0,nil} is the only allowed empty entry
	for _, cmd := range cmds {
		if len(cmd) == 0 {
			s.mu.Unlock()
			return nil, errors.New("empty command not allowed")
		}
		if len(cmd) > MaxCommandSize {
			s.mu.Unlock()
			return nil, errors.New("command too large")
		}
	}

	slog.Debug("Processing new commands", "commands", len(cmds))

	// Append the command(s) to the log
	var newEntries []LogEntry
	for _, cmd := range cmds {
		entry := LogEntry{
			Term:    s.CurrentTerm,
			Command: cmd,
		}
		s.Log = append(s.Log, entry)
		newEntries = append(newEntries, entry)
	}

	if err := s.storage.AppendToLog(ctx, newEntries...); err != nil {
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			s.mu.Unlock()
			return nil, ctx.Err()
		}
		slog.Error("Failed to persist new entries on disk", "error", err)
		panic("Failed to persist new entries on disk")
	}

	quorumChan := make(chan struct{}, 1)
	var confirmed atomic.Int32
	confirmed.Store(1) // Leader already confirmed

	quorumSize := s.getQuorumSize()
	if quorumSize <= 1 {
		quorumChan <- struct{}{}
	}
	s.mu.Unlock()

	// Issue AppendEntries RPCs in parallel to each of the other servers to replicate the entry
	for _, member := range s.clusterMembers {
		if member.Id == s.id {
			continue
		}

		go func(member *ClusterMember) {
			// Todo: Add a limit to the number of entries that can be sent in a single RPC
			// Todo: This retry loop should have an exponential backoff or something

			for {
				if ctx.Err() != nil {
					return
				}

				s.mu.Lock()
				next := member.nextIndex
				prevLogIndex := next - 1
				prevLogTerm := s.Log[prevLogIndex].Term

				var entries []LogEntry
				logLen := uint64(len(s.Log) - 1)
				if logLen >= next {
					for _, e := range s.Log[next:] {
						entries = append(entries, LogEntry{
							Term:    e.Term,
							Command: e.Command,
						})
					}
				}

				req := &AppendEntriesRequest{
					Term:         (s.CurrentTerm),
					LeaderId:     (s.id),
					LeaderCommit: s.commitIndex,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  (prevLogTerm),
					Entries:      entries,
				}
				s.mu.Unlock()

				resp, err := s.transport.AppendEntries(ctx, member.Id, req)
				if err != nil {
					slog.Error("Error replicating entry", "server", member.Id, "error", err)
					select {
					case <-ctx.Done():
						return
					case <-time.After(20 * time.Millisecond):
						continue
					}
				}

				term := Term(resp.Term)
				if s.checkResponseTerm(term) {
					break
				}

				if !resp.Success {
					s.mu.Lock()
					slog.Warn("Failed to replicate entry", "server", member.Id, "followerTerm", term, "leaderTerm", s.CurrentTerm)
					if member.nextIndex == 0 {
						slog.With("Follower is missing entries and Leader has no more entries to send", "member", member.Id)
						panic("Follower is missing entries and Leader has no more entries to send")
					}
					if member.nextIndex > 1 {
						member.nextIndex--
					}
					s.mu.Unlock()
				} else {
					s.mu.Lock()
					member.matchIndex = prevLogIndex + uint64(len(entries))
					member.nextIndex = member.matchIndex + 1
					s.maybeAdvanceCommitIndex()
					s.mu.Unlock()

					// Entry was successfully replicated
					if int(confirmed.Add(1)) == quorumSize {
						select {
						case quorumChan <- struct{}{}:
						default:
						}
					}
					break
				}
			}
		}(member)
	}
	// Wait for a quorum of servers to confirm the entry
	select {
	case <-quorumChan:
		s.mu.Lock()
		s.maybeAdvanceCommitIndex()
		s.mu.Unlock()
		s.sendHeartbeat(ctx)
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Return the result of that execution to the client, this can't return an error as the command is already committed.
	res := s.stateMachine.Apply(cmds...)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastApplied += uint64(len(cmds))
	return res, nil
}

func (s *Server) requestVoteFromMember(ctx context.Context, member *ClusterMember) {
	s.mu.Lock()
	slog.Debug("Requesting vote from server", "server", member.Id)
	lastLogIndex := uint64(len(s.Log) - 1)
	lastLogTerm := s.Log[lastLogIndex].Term

	req := &RequestVoteRequest{
		Term:         (s.CurrentTerm),
		CandidateId:  (s.id),
		LastLogIndex: lastLogIndex,
		LastLogTerm:  (lastLogTerm),
	}

	s.mu.Unlock()

	resp, err := s.transport.RequestVote(ctx, member.Id, req)
	if err != nil || ctx.Err() != nil {
		if err != nil {
			slog.Error("Error requesting vote", "member", member.Id, "error", err)
		}
		// This will be retried on the next election timer tick
		return
	}
	s.mu.Lock()

	defer s.mu.Unlock()
	slog.Debug("Received vote response", "server", member.Id, "voteGranted", resp.VoteGranted, "term", resp.Term)

	if err := s.updateTerm(ctx, resp.Term); err != nil {
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			return
		}
		panic("Failed to write metadata to disk")
	}

	if resp.Term != req.Term {
		// This is an invalid response - no op
		return
	}

	if resp.VoteGranted {
		slog.Debug("Received vote from server", "server", member.Id)
		member.votedFor = s.id
		s.checkIfElected(ctx)
	}
}

// Must be called with the lock held
func (s *Server) checkIfElected(ctx context.Context) {
	if s.state == Candidate {
		// If we're a Candidate we need to check if we've received a majority of votes
		// If we have, we become the Leader

		quorum := s.getQuorumSize()
		slog.Debug("Checking if elected", "quorumSize", quorum)
		votesReceived := 0
		for _, member := range s.clusterMembers {
			if member.votedFor == s.id {
				votesReceived++
			}
			if votesReceived == quorum {
				slog.Debug("Received quorum of votes, transitioning to Leader")
				if s.state == Follower {
					panic("Invalid state transition from Follower to Leader")
				}
				s.state = Leader
				s.electionTicker.Stop()
				s.heartbeatTicker.Reset(heartbeatTimeout)

				s.initializeVolatileLeaderState()

				// Send initial empty AppendEntries RPCs to all other servers
				// Include the Term in the RPC
				// If followers are up-to-date, they will respond with success
				s.sendHeartbeat(ctx)
			} else {
				slog.Debug("Not enough votes yet", "quorumSize", quorum, "votesReceived", votesReceived)
			}
		}
	}
}

func (s *Server) getQuorumSize() int {
	// This doesn't change at the moment, but it's a good idea to have a function for
	// it as raft can support dynamic cluster membership
	return (len(s.clusterMembers) + 1) / 2
}

func (s *Server) initializeVolatileLeaderState() {
	for _, m := range s.clusterMembers {
		// NextIndex for each server is initialized to the Leader's last log index + 1
		m.nextIndex = uint64(len(s.Log))
		// MatchIndex for each server is initialized to 0
		// This is the highest index in the Leader's log that the Follower has confirmed is replicated
		m.matchIndex = 0
	}

}

func (s *Server) sendHeartbeat(ctx context.Context) {
	for _, member := range s.clusterMembers {
		if member.Id == s.id {
			continue
		}
		go func(ctx context.Context, member *ClusterMember) {
			s.mu.Lock()
			if s.state != Leader {
				s.mu.Unlock()
				return
			}

			next := member.nextIndex
			if next == 0 {
				next = 1
			}
			prevLogIndex := next - 1
			prevLogTerm := s.Log[prevLogIndex].Term

			var entries []LogEntry
			logLen := uint64(len(s.Log) - 1)
			if logLen >= next {
				for _, e := range s.Log[next:] {
					entries = append(entries, LogEntry{
						Term:    e.Term,
						Command: e.Command,
					})
				}
			}

			req := &AppendEntriesRequest{
				Term:         s.CurrentTerm,
				LeaderId:     (s.id),
				LeaderCommit: s.commitIndex,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  (prevLogTerm),
				Entries:      entries,
			}
			s.mu.Unlock()

			resp, err := s.transport.AppendEntries(ctx, member.Id, req)
			if err != nil || ctx.Err() != nil {
				if err != nil {
					slog.Error("Error sending heartbeat", "error", err)
				}
				return
			}

			if s.checkResponseTerm(Term(resp.Term)) {
				return
			}

			s.mu.Lock()
			defer s.mu.Unlock()
			if s.state != Leader || Term(resp.Term) != s.CurrentTerm {
				return
			}

			if resp.Success {
				member.matchIndex = prevLogIndex + uint64(len(entries))
				member.nextIndex = member.matchIndex + 1
			} else {
				if member.nextIndex > 1 {
					member.nextIndex--
				}
			}
		}(ctx, member)
	}
}

func (s *Server) checkResponseTerm(respTerm Term) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	term := s.CurrentTerm
	if respTerm > term {
		slog.Info("Transitioning to Follower", "Term", term)
		s.heartbeatTicker.Stop()
		s.CurrentTerm = respTerm
		s.state = Follower
		s.VotedFor = 0
		s.resetElectionTimer()
		return true
	}
	return false
}

func (s *Server) getMemberById(id MemberId) *ClusterMember {
	if member, ok := s.clusterMembers[id]; ok {
		return member
	}
	panic("Member not found")
}

func (s *Server) State() ServerState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// Must be called with the lock held
func (s *Server) maybeAdvanceCommitIndex() uint64 {
	matchIndexes := make([]uint64, 0, len(s.clusterMembers))
	for k, v := range s.clusterMembers {
		if k == s.id {
			matchIndexes = append(matchIndexes, uint64(len(s.Log)-1))
		} else {
			matchIndexes = append(matchIndexes, v.matchIndex)
		}
	}
	slices.Sort(matchIndexes)
	majorityIndex := matchIndexes[len(matchIndexes)/2]
	if majorityIndex > s.commitIndex && s.Log[majorityIndex].Term == s.CurrentTerm {
		s.commitIndex = majorityIndex
	}
	return s.commitIndex
}
