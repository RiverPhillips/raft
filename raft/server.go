// Package raft server implementation
package raft

import (
	"connectrpc.com/connect"
	"context"
	"crypto/rand"
	"fmt"
	v1 "github.com/RiverPhillips/raft/gen/proto/raft/v1"
	"github.com/RiverPhillips/raft/gen/proto/raft/v1/raftv1connect"
	"log/slog"
	"math/big"
	"net/http"
	"sync"
	"time"
)

const (
	heartbeatTimeout   = time.Millisecond * 50
	minElectionTimeout = 150
)

type Server struct {
	raftv1connect.UnimplementedRaftServiceHandler

	mu              sync.Mutex
	id              MemberId
	electionTicker  *time.Ticker
	heartbeatTicker *time.Ticker

	clusterMembers []*ClusterMember

	// Persistent state on all servers
	currentTerm Term
	votedFor    MemberId
	log         []LogEntry // This is indexed from 1

	// Volatile state on all servers
	commitIndex uint64
	lastApplied uint64
	state       ServerState

	leader *ClusterMember

	stateMachine StateMachine
}

func getElectionTimeout() time.Duration {
	r, err := rand.Int(rand.Reader, big.NewInt(150))
	if err != nil {
		panic("Failed to generate random number")
	}
	return time.Millisecond * time.Duration(minElectionTimeout+r.Int64())
}

func NewServer(id MemberId, sm StateMachine, members []*ClusterMember) *Server {
	if id < 1 {
		panic("Server ID must be an integer greater than 0")
	}

	if len(members)%2 == 0 {
		panic("Cluster must have an odd number of members")
	}

	hbTicker := time.NewTicker(heartbeatTimeout)
	hbTicker.Stop()

	for _, member := range members {
		member.rpcClient = raftv1connect.NewRaftServiceClient(
			http.DefaultClient, // Todo - pass in a custom client
			fmt.Sprintf("http://%s", member.Addr),
			connect.WithGRPC(),
		)
	}

	return &Server{
		state:           Follower,
		id:              id,
		electionTicker:  time.NewTicker(getElectionTimeout()),
		heartbeatTicker: hbTicker,
		clusterMembers:  members,
		log:             []LogEntry{{}}, // Todo: Load from disk
		stateMachine:    sm,
	}
}

// AppendEntries is an RPC method that is called by the Leader to replicate log entries
func (s *Server) AppendEntries(ctx context.Context, connReq *connect.Request[v1.AppendEntriesRequest]) (*connect.Response[v1.AppendEntriesResponse], error) {
	// 1. Reply false if Term < currentTerm
	s.mu.Lock()
	defer s.mu.Unlock()
	req := connReq.Msg

	s.updateTerm(Term(req.Term))

	resp := &v1.AppendEntriesResponse{}

	// If AppendEntries RPC received from new Leader: convert to Follower
	if s.state == Candidate {
		s.state = Follower
		slog.Debug("Transitioning to Follower", "Term", s.currentTerm)
	}

	if s.state != Follower {
		panic("Only followers should be receiving append entries")
	}

	leaderId := NewMemberId(req.LeaderId)

	if s.leader == nil || leaderId != s.leader.Id {
		s.leader = s.getMemberById(leaderId)
	}

	resp.Term = uint64(s.currentTerm)
	resp.Success = false

	// Reply false if term < currentTerm
	if Term(req.Term) < s.currentTerm {
		// This is a stale request from an old Leader
		slog.Debug("Rejecting append entries request from stale Leader", "server", s.id, "term", req.Term, "currentTerm", s.currentTerm, "Leader", req.LeaderId)
		return connect.NewResponse(resp), nil
	}

	// We have a valid Leader
	s.resetElectionTimer()

	prevLogTerm := Term(req.PrevLogTerm)
	slog.Debug("Received append entries request from valid Leader", "server", s.id, "term", req.Term, "Leader", req.LeaderId, "prevLogIndex", req.PrevLogIndex, "prevLogTerm", prevLogTerm, "entries", len(req.Entries))

	// 2. Reply false if log does not contain an entry at prevLogIndex whose Term matches prevLogTerm
	logLen := uint64(len(s.log))
	validLog := req.PrevLogIndex == 0 || (req.PrevLogIndex < logLen && s.log[req.PrevLogIndex].Term == prevLogTerm)

	if !validLog {
		slog.Debug("Rejecting append entries request. Log was not valid", "server", s.id, "term", req.Term, "Leader", req.LeaderId, "prevLogIndex", req.PrevLogIndex, "prevLogTerm", prevLogTerm, "logLength", logLen)
		return connect.NewResponse(resp), nil
	}

	nextIdx := req.PrevLogIndex + 1

	for i := nextIdx; i < nextIdx+uint64(len(req.Entries)); i++ {
		ent := req.Entries[i-nextIdx]
		entry := LogEntry{
			Term:    Term(ent.Term),
			Command: ent.Command,
		}
		if i >= uint64(cap(s.log)) {
			// We're at the capacity of the log let's increase the capacity
			newLen := nextIdx + uint64(len(req.Entries))
			newLog := make([]LogEntry, i, newLen*2)
			copy(newLog, s.log)
			s.log = newLog
		} else if logLen > i && s.log[i].Term != entry.Term {
			s.log = s.log[:i]
			slog.Debug("Deleted conflicting entries from log", "server", s.id, "term", req.Term, "Leader", req.LeaderId, "index", i, "logLength", len(s.log))
		}

		slog.Debug("Appending entry to log", "server", s.id, "term", req.Term, "Leader", req.LeaderId, "index", i, "logLength", len(s.log))
		if i < uint64(len(s.log)) {
			slog.Debug("Log is unchanged")
		} else {
			s.log = append(s.log, entry)
		}
	}

	if req.LeaderCommit > s.commitIndex {
		s.commitIndex = min(req.LeaderCommit, logLen)
	}

	// Todo: update state on disk

	resp.Success = true

	return connect.NewResponse(resp), nil
}

// Must be called with the lock held
func (s *Server) updateTerm(term Term) bool {
	if term > s.currentTerm {
		s.state = Follower
		s.currentTerm = term
		s.votedFor = 0
		// Todo: update state on disk
		s.resetElectionTimer()
		return true
	}
	return false
}

// Must be called with the lock held
func (s *Server) resetElectionTimer() {
	s.electionTicker.Reset(getElectionTimeout())
}

// RequestVote is an RPC method that is called by candidates to gather votes
func (s *Server) RequestVote(ctx context.Context, connReq *connect.Request[v1.RequestVoteRequest]) (*connect.Response[v1.RequestVoteResponse], error) {
	req := connReq.Msg
	reqTerm := Term(req.Term)
	candidateId := MemberId(req.CandidateId)
	slog.Info("Received request for vote", "server", s.id, "term", reqTerm, "Candidate", candidateId)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updateTerm(reqTerm)

	resp := &v1.RequestVoteResponse{}

	resp.Term = uint64(s.currentTerm)
	resp.VoteGranted = false

	if reqTerm < s.currentTerm {
		slog.Debug("Rejecting vote request. Term not valid", "server", s.id, "term", reqTerm, "Candidate", candidateId, "currentTerm", s.currentTerm)
		return connect.NewResponse(resp), nil
	}

	logLen := len(s.log) - 1
	lastLogTerm := Term(req.LastLogTerm)
	logValid := lastLogTerm > s.log[logLen].Term || req.LastLogIndex >= uint64(logLen)
	grantVote := reqTerm >= s.currentTerm && (s.votedFor == 0 || s.votedFor == candidateId) && logValid

	if grantVote {
		slog.Debug("Voting for server", "server", candidateId)
		s.votedFor = candidateId
		resp.VoteGranted = true
		s.resetElectionTimer()
		return connect.NewResponse(resp), nil
	} else {
		slog.Debug("Rejecting vote request. Log was not up to date enough", "server", s.id, "term", reqTerm, "Candidate", req.CandidateId, "votedFor", s.votedFor, "lastLogIndex", req.LastLogIndex, "logLength", logLen)
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) Start(ctx context.Context) error {
	// todo: Load state from disk

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
			s.currentTerm++
			s.votedFor = s.id

			for _, member := range s.clusterMembers {
				member.votedFor = 0
				if member.Id == s.id {
					// Vote for ourselves, we don't need to send a request to ourselves
					member.votedFor = s.id
					s.resetElectionTimer()
					continue
				}

				go s.requestVoteFromMember(member)
			}
			s.checkIfElected()
			s.mu.Unlock()
			// Send RequestVote RPCs to all other servers
			// If votes received from a quorum of servers: become Leader
		case <-s.heartbeatTicker.C:
			s.mu.Lock()
			if s.state != Leader {
				panic("Only leaders should be sending heartbeats")
			}
			s.mu.Unlock()
			slog.Debug("Sending heartbeat")
			s.sendHeartbeat()
		}
	}
}

func (s *Server) ApplyCommand(cmds ...Command) ([]Result, error) {
	slog.Debug("Received command", "commands", len(cmds))
	s.mu.Lock()

	if s.state != Leader {
		s.mu.Unlock()
		return nil, &NotLeaderError{LeaderId: s.leader.Id, LeaderAddr: s.leader.Addr}
	}

	slog.Debug("Processing new commands", "commands", len(cmds))

	// Append the command(s) to the log
	for _, cmd := range cmds {
		s.log = append(s.log, LogEntry{
			Term:    s.currentTerm,
			Command: cmd,
		})
		s.commitIndex++
	}

	// Todo: persist to disk

	var wg sync.WaitGroup
	wg.Add(s.getQuorumSize())
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
				s.mu.Lock()
				next := member.nextIndex
				prevLogIndex := next - 1
				prevLogTerm := s.log[prevLogIndex].Term

				var entries []*v1.LogEntry
				logLen := uint64(len(s.log) - 1)
				if logLen >= next {
					for _, e := range s.log[next:] {
						entries = append(entries, &v1.LogEntry{
							Term:    uint64(e.Term),
							Command: e.Command,
						})
					}
				}

				req := &v1.AppendEntriesRequest{
					Term:         uint64(s.currentTerm),
					LeaderId:     uint32(s.id),
					LeaderCommit: s.commitIndex,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  uint64(prevLogTerm),
					Entries:      entries,
				}
				s.mu.Unlock()

				connResp, err := member.rpcClient.AppendEntries(context.Background(), connect.NewRequest(req))
				if err != nil {
					slog.Error("Error replicating entry", "server", member.Id, "error", err, "success", connResp.Msg.Success)
				}

				resp := connResp.Msg

				term := Term(resp.Term)
				if s.checkResponseTerm(term) {
					break
				}

				if !resp.Success {
					slog.Error("Failed to replicate entry", "server", member.Id, "followerTerm", term, "leaderTerm", s.currentTerm)
					if member.nextIndex == 0 {
						slog.With("Follower is missing entries and Leader has no more entries to send", "member", member.Id)
						panic("Follower is missing entries and Leader has no more entries to send")
					}
					member.nextIndex--
				} else {
					s.mu.Lock()
					s.commitIndex = logLen
					member.nextIndex++
					member.matchIndex = s.commitIndex
					s.mu.Unlock()
					// Entry was successfully replicated
					break
				}
			}
			wg.Done()
		}(member)
	}
	// Wait for a quorum of servers to confirm the entry
	wg.Wait()

	// Return the result of that execution to the client, this can't return an error as the command is already committed.
	res := s.stateMachine.Apply(cmds...)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastApplied++
	return res, nil
}

func (s *Server) requestVoteFromMember(member *ClusterMember) {
	s.mu.Lock()
	slog.Debug("Requesting vote from server", "server", member.Id)
	logLen := uint64(len(s.log) - 1)
	lastLogTerm := Term(0)
	if logLen > 0 {
		lastLogTerm = s.log[logLen-1].Term
	}

	req := &v1.RequestVoteRequest{
		Term:         uint64(s.currentTerm),
		CandidateId:  uint32(s.id),
		LastLogIndex: logLen,
		LastLogTerm:  uint64(lastLogTerm),
	}

	s.mu.Unlock()

	connResp, err := member.rpcClient.RequestVote(context.TODO(), connect.NewRequest(req))
	if err != nil {
		slog.Error("Error requesting vote", "member", member.Id, "error", err)
		// This will be retried on the next election timer tick
		return
	}
	s.mu.Lock()

	resp := connResp.Msg

	defer s.mu.Unlock()
	slog.Debug("Received vote response", "server", member.Id, "voteGranted", resp.VoteGranted, "term", resp.Term)

	if s.updateTerm(Term(resp.Term)) {
		return
	}

	if resp.Term != req.Term {
		// This is an invalid response - no op
		return
	}

	if resp.VoteGranted {
		slog.Debug("Received vote from server", "server", member.Id)
		member.votedFor = s.id
		s.checkIfElected()
	}
}

// Must be called with the lock held
func (s *Server) checkIfElected() {
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
				s.sendHeartbeat()
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
		m.nextIndex = uint64(len(s.log))
		// MatchIndex for each server is initialized to 0
		// This is the highest index in the Leader's log that the Follower has confirmed is replicated
		m.matchIndex = 0
	}

}

func (s *Server) sendHeartbeat() {
	for _, member := range s.clusterMembers {
		if member.Id == s.id {
			continue
		}
		go func(member *ClusterMember) {
			s.mu.Lock()

			prevLogIndex := uint64(len(s.log) - 1)
			prevLogTerm := s.log[prevLogIndex].Term

			req := &v1.AppendEntriesRequest{
				Term:         uint64(s.currentTerm),
				LeaderId:     uint32(s.id),
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  uint64(prevLogTerm),
				Entries:      []*v1.LogEntry{},
			}
			s.mu.Unlock()

			resp, err := member.rpcClient.AppendEntries(context.TODO(), connect.NewRequest(req))
			if err != nil {
				slog.Error("Error sending heartbeat", "error", err)
				return
			}

			_ = s.checkResponseTerm(Term(resp.Msg.Term))
		}(member)
	}
}

func (s *Server) checkResponseTerm(respTerm Term) bool {
	if respTerm > s.currentTerm {
		slog.Debug("Transitioning to Follower", "Term", s.currentTerm)
		s.mu.Lock()
		defer s.mu.Unlock()
		s.currentTerm = respTerm
		s.state = Follower
		s.votedFor = 0
		s.resetElectionTimer()
		return true
	}
	return false
}

func (s *Server) getMemberById(id MemberId) *ClusterMember {
	// Todo: Do we need a map or is it so small it's irrelevant?
	for _, member := range s.clusterMembers {
		if member.Id == id {
			return member
		}
	}
	panic("Member not found")
}

func (s *Server) State() ServerState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}
