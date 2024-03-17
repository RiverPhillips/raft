// Package raft server implementation
package raft

import (
	"context"
	"log/slog"
	"math/rand"
	"net/rpc"
	"sync"
	"time"
)

const (
	heartbeatTimeout   = time.Millisecond * 500
	minElectionTimeout = 1500
)

type Server struct {
	mu              sync.Mutex
	id              memberId
	electionTicker  *time.Ticker
	heartbeatTicker *time.Ticker

	clusterMembers []*ClusterMember

	// Persistent state on all servers
	currentTerm Term
	votedFor    memberId
	log         []LogEntry // This is indexed from 1

	// Volatile state on all servers
	commitIndex uint64
	lastApplied uint64
	state       serverState

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

func getElectionTimeout() time.Duration {
	return time.Millisecond * time.Duration(minElectionTimeout+rand.Intn(150))
}

func NewServer(id memberId, members []*ClusterMember) *Server {
	if id < 1 {
		panic("Server ID must be an integer greater than 0")
	}

	if len(members)%2 == 0 {
		panic("Cluster must have an odd number of members")
	}

	hbTicker := time.NewTicker(heartbeatTimeout)
	hbTicker.Stop()

	return &Server{
		state:           follower,
		id:              id,
		electionTicker:  time.NewTicker(getElectionTimeout()),
		heartbeatTicker: hbTicker,
		clusterMembers:  members,
		log:             []LogEntry{{}}, // Todo: Load from disk
	}
}

// AppendEntries is an RPC method that is called by the leader to replicate log entries
func (s *Server) AppendEntries(req *AppendEntriesRequest, resp *AppendEntriesResponse) error {
	// 1. Reply false if Term < currentTerm
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.Term < s.currentTerm {
		// This is a stale request from an old leader
		resp.Term = s.currentTerm
		s.mu.Unlock()
		resp.Success = false
		return nil
	}

	s.resetElectionTimer()

	// If RPC request or response contains Term T >= currentTerm: set currentTerm = T, convert to follower
	if req.Term >= s.currentTerm && s.state == candidate {
		s.state = follower
		s.currentTerm = req.Term
		s.votedFor = 0
		slog.Debug("Transitioning to follower", "Term", s.currentTerm)
	}

	// 2. Reply false if log does not contain an entry at prevLogIndex whose Term matches prevLogTerm
	logLen := uint64(len(s.log) - 1)
	if (logLen) < req.PrevLogIndex || s.log[req.PrevLogIndex].Term != req.PrevLogTerm {
		resp.Term = s.currentTerm
		s.mu.Unlock()
		resp.Success = false
		return nil
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	if logLen > req.PrevLogIndex+1 {
		s.log = s.log[:req.PrevLogIndex+1]
	}

	// 4. Append any new entries not already in the log
	// Check which entries are new

	if req.LeaderCommit > s.commitIndex {
		s.commitIndex = min(req.LeaderCommit, logLen)
	}

	resp.Term = s.currentTerm
	resp.Success = true

	return nil
}

// Must be called with the lock held
func (s *Server) resetElectionTimer() {
	s.electionTicker.Reset(getElectionTimeout())
}

// RequestVote is an RPC method that is called by candidates to gather votes
func (s *Server) RequestVote(req *RequestVoteRequest, resp *RequestVoteResponse) error {
	slog.Info("Received request for vote", "server", s.id, "term", req.Term, "candidate", req.CandidateID)
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.Term > s.currentTerm {
		s.currentTerm = req.Term
		s.state = follower
		s.votedFor = 0
		s.resetElectionTimer()
	}

	resp.Term = s.currentTerm
	resp.VoteGranted = false

	if req.Term < s.currentTerm {
		slog.Debug("Rejecting vote request. Term not valid", "server", s.id, "term", req.Term, "candidate", req.CandidateID, "currentTerm", s.currentTerm)
		return nil
	}

	logLen := len(s.log) - 1
	logValid := req.LastLogTerm > s.log[logLen].Term || req.LastLogIndex >= uint64(logLen)
	grant := req.Term == s.currentTerm && (s.votedFor == 0 || s.votedFor == req.CandidateID) && logValid

	if grant {
		slog.Debug("Voting for server", "server", req.CandidateID)
		s.votedFor = req.CandidateID
		resp.VoteGranted = true
		s.resetElectionTimer()
		return nil
	} else {
		slog.Debug("Rejecting vote request. Log was not up to date enough", "server", s.id, "term", req.Term, "candidate", req.CandidateID, "votedFor", s.votedFor, "lastLogIndex", req.LastLogIndex, "logLength", logLen)
	}
	return nil
}

func (s *Server) Start(ctx context.Context) error {
	s.state = follower

	// todo: Load state from disk

	slog.Debug("Starting server as follower")

	// Start the election timer
	// If the election timer elapses without receiving AppendEntries RPC from the current leader or granting a vote to another candidate, convert to candidate

	for {
		select {
		case <-ctx.Done():
			slog.Debug("Shutting down raft server")
			return nil
		case <-s.electionTicker.C:
			if s.state == leader {
				// If we're the leader we shouldn't be running an election timer
				// Something has gone wrong
				panic("Leader running election timer")
			}

			slog.Debug("Election timer elapsed, transitioning to candidate")
			s.mu.Lock()
			s.state = candidate
			s.currentTerm++
			s.votedFor = s.id
			s.mu.Unlock()

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

			// Send RequestVote RPCs to all other servers
			// If votes received from a quorum of servers: become leader
		case <-s.heartbeatTicker.C:
			if s.state != leader {
				panic("Only leaders should be sending heartbeats")
			}
			slog.Debug("Sending heartbeat")
			s.sendHeartbeat()
		}
	}
}

func (s *Server) requestVoteFromMember(member *ClusterMember) {
	slog.Debug("Requesting vote from server", "server", member.Id)

	logLen := uint64(len(s.log) - 1)

	req := &RequestVoteRequest{
		Term:         s.currentTerm,
		CandidateID:  s.id,
		LastLogIndex: logLen,
	}

	resp := &RequestVoteResponse{}
	if err := s.makeRpcCall(member, "Server.RequestVote", req, resp); err != nil {
		slog.Error("Error requesting vote", "member", member.Id, "error", err)
		// This will be retried on the next election timer tick
		return
	}

	slog.Debug("Received vote response", "server", member.Id, "voteGranted", resp.VoteGranted, "term", resp.Term)

	// If RPC request or response contains Term T > currentTerm: set currentTerm = T, convert to follower
	if resp.Term > s.currentTerm {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.currentTerm = resp.Term
		s.state = follower
		s.votedFor = 0
		s.resetElectionTimer()
		slog.Debug("Transitioning to follower", "Term", s.currentTerm)
		// Todo: update state on disk
		return
	}

	if resp.Term != req.Term {
		// This is a stale response - no op
		return
	}

	if resp.VoteGranted {
		slog.Debug("Received vote from server", "server", member.Id)
		member.votedFor = s.id
		s.checkIfElected()
	}
}

func (s *Server) checkIfElected() {
	if s.state == candidate {
		// If we're a candidate we need to check if we've received a majority of votes
		// If we have, we become the leader

		quorum := (len(s.clusterMembers) + 1) / 2
		slog.Debug("Checking if elected", "quorumSize", quorum)
		votesReceived := 0
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, member := range s.clusterMembers {
			if member.votedFor == s.id {
				votesReceived++
			}
			if votesReceived == quorum {
				slog.Debug("Received quorum of votes, transitioning to leader")
				s.state = leader
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

func (s *Server) initializeVolatileLeaderState() {
	// NextIndex for each server is initialized to the leader's last log index + 1
	s.nextIndex = make([]int, len(s.clusterMembers))
	for i := range s.nextIndex {
		s.nextIndex[i] = len(s.log)
	}
	// MatchIndex for each server is initialized to 0
	// This is the highest index in the leader's log that the follower has confirmed is replicated
	s.matchIndex = make([]int, len(s.clusterMembers))
}

func (s *Server) sendHeartbeat() {
	for _, member := range s.clusterMembers {
		if member.Id == s.id {
			continue
		}
		go func(member *ClusterMember) {
			resp := &AppendEntriesResponse{}
			term := Term(0)
			if s.log != nil && len(s.log) > 0 {
				term = s.log[len(s.log)-1].Term
			}
			err := s.makeRpcCall(member, "Server.AppendEntries", &AppendEntriesRequest{
				Term:         s.currentTerm,
				LeaderID:     s.id,
				PrevLogIndex: uint64(len(s.log) - 1),
				PrevLogTerm:  term,
				Entries:      []LogEntry{},
			}, resp)

			if err != nil {
				slog.Error("Error sending heartbeat", "error", err)
				return
			}

			if resp.Term > s.currentTerm {
				slog.Debug("Transitioning to follower", "Term", s.currentTerm)

				s.mu.Lock()
				defer s.mu.Unlock()
				s.currentTerm = resp.Term
				s.state = follower
				s.votedFor = 0
				s.resetElectionTimer()
			}
		}(member)
	}
}

func (s *Server) makeRpcCall(member *ClusterMember, methodName string, req any, resp any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var err error
	if member.rpcClient == nil {
		member.rpcClient, err = rpc.DialHTTP("tcp", member.Addr)
		if err != nil {
			slog.Warn("Error dialing member", "error", err, "member", member.Id)
			return err
		}
	}

	err = member.rpcClient.Call(methodName, req, resp)
	if err != nil {
		slog.Warn("Error calling rpc", "error", err, "member", member.Id)
		return err
	}

	return nil
}
