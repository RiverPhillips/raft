# AGENTS.md

From-scratch Raft library in Go. Educational / learning project: the goal is to learn Raft deeply.
Code should be hand-written by the author — agents should provide design reviews, explanations, trade-off analysis, and debugging hints, NOT write or modify code unless explicitly directed.

This is a library, not a process. There is no `main.go`, Docker, HTTP server, or observability. Prove behavior with tests.

Module: `github.com/RiverPhillips/raft`
Go: 1.27

## Layout

```
raft/                   # consensus core
  server.go             # Server, RPCs, election, replication, ApplyCommand
  types.go              # Term, LogEntry, ServerState, ClusterMember, Command, StateMachine, Transport, Storage, PersistentState
  state_machine.go      # NoOpStateMachine (tests)
  server_test.go        # unit tests (RequestVote, AppendEntries, ApplyCommand)
  cluster_test.go       # in-process cluster integration tests (elections, replication, failover)
  storage/              # durable storage implementations
    ondiskstorage.go    # OnDiskStorage (WAL + atomic metadata)
    ondiskstorage_test.go
kv/sm.go                # in-memory KV StateMachine + binary command encoding
justfile                # just test, just test-repeat, just stress
.github/workflows/ci.yml
```

Packages:

- `raft` — consensus core. Depend on this.
- `raft/storage` — storage implementations (e.g. `OnDiskStorage`).
- `kv` — optional `raft.StateMachine` used as a real SM in tests. Not a service.

Do not invent packages. Do not reintroduce `main`, Docker, OTEL, or a demo loop unless that is the task. There is no client library, no membership-change RPC, and no persistence layer.

## What exists vs what does not

Implemented:

- Follower / Candidate / Leader state transitions
- RequestVote and AppendEntries over injected `Transport` interface (`InMemoryTransport`)
- Randomized election timeout (150–299ms), heartbeats (50ms)
- Log append + conflict truncate on followers
- Channel-based quorum wait in leader `ApplyCommand(ctx, cmds...)` + parallel AppendEntries to peers
- Follower redirect via `NotLeaderError` (LeaderId)
- Follower commit index advancement and apply loop to `StateMachine`
- In-process 3-node cluster tests in `cluster_test.go` (election, replication, leader failover)
- KV get/set encoding
- `NewServer(id, sm, members, transport, opts...)` functional options

Not implemented (do not pretend they work):

- Persistent `currentTerm` / `votedFor` / log (`raft/storage` in progress)
- Correct leader commit-index advancement purely from follower `matchIndex` (leader currently bumps `commitIndex` locally upon appending)
- Snapshotting, log compaction, dynamic membership changes
- Bounded AppendEntries batches / backoff on retry

## Commands

```bash
go test ./...
go test -race -coverprofile=coverage.txt -covermode=atomic ./...   # CI
go test -race -count=20 ./raft
just test
just test-repeat 50
just stress TestElectsALeader
```

IDs must be >= 1. Cluster size must be odd (`NewServer` panics otherwise).

## RPC / Transport / Storage

RPCs are defined as Go struct types and handled via the `Transport` interface (`raft/types.go`):

- `AppendEntries(ctx context.Context, to MemberId, req *AppendEntriesRequest) (*AppendEntriesResult, error)`
- `RequestVote(ctx context.Context, to MemberId, req *RequestVoteRequest) (*RequestVoteResult, error)`

`Server` handles incoming RPCs via direct method calls (`AppendEntries`, `RequestVote`) and sends outgoing RPCs via its injected `Transport`.

Storage is defined via the `Storage` interface (`raft/types.go`):

- `WriteMetadata(ctx context.Context, currentTerm Term, votedFor MemberId) error`
- `AppendToLog(ctx context.Context, logs ...LogEntry) error`
- `LoadState(ctx context.Context) (PersistentState, error)`

State struct: `PersistentState` (`CurrentTerm`, `VotedFor`, `Log`). Embeds directly on `Server`.
Implementation: `raft/storage.OnDiskStorage`.

KV command bytes (`kv/sm.go`): first byte `0` = get, `1` = set; then big-endian u32 key length + key; set also has u32 value length + value. `StateMachine.Apply(commands ...Command) []Result` must return one result per command.

## Consensus invariants to preserve

These are load-bearing. Do not "simplify" them away.

- Log is 1-indexed: `log[0]` is a dummy `{Term:0}` sentinel. `len(log)-1` is last index.
- Member IDs are `uint32` starting at 1 (`NewMemberId` panics on 0).
- Cluster membership is a static odd-sized map; quorum is `(len(members)+1)/2`.
- Heartbeat interval: 50ms. Election timeout: 150–299ms (`crypto/rand`).
- `Start` blocks on election/heartbeat tickers until `ctx` cancel. Illegal transitions panic (Leader -> Candidate, Follower -> Leader).
- `ApplyCommand` on a non-leader returns `*NotLeaderError` with message `"not the Leader"`.
- Several methods document "must be called with the lock held" (`updateTerm`, `resetElectionTimer`, `checkIfElected`, `initializeVolatileLeaderState`). Honor that. `checkResponseTerm` takes the lock itself.
- RPC methods and `ApplyCommand` take `context.Context` as the first argument. Pass it through; do not swap in `context.Background()` / `context.TODO()` on live paths.

Known sharp edges & fixed areas:

- `ApplyCommand` uses an atomic confirmation counter + channel instead of an unbalanced `WaitGroup`.
- `requestVoteFromMember` uses `s.log[logLen-1]` after `logLen := len(s.log)-1` (off-by-one vs last entry).
- `Start` allocates an unused vote `WaitGroup` (Add without Done/Wait).
- `AppendEntries` Candidate step-down: candidates receiving `req.Term >= s.currentTerm` step down to `Follower`.
- `sendHeartbeat` sets `LeaderCommit: s.commitIndex` so followers advance commitIndex on heartbeat ticks.
- `checkResponseTerm` takes `s.mu` to avoid race conditions.

## Tests

Tests live in `raft/server_test.go` (single-node RPC tests) and `raft/cluster_test.go` (in-memory multi-node cluster tests).

Cluster test suite (`cluster_test.go`):
- `TestElectsALeader`: 3-node election convergence, term agreement, leader recognition.
- `TestTheSameCommandsGetsAppliesToAllMembers`: Leader `ApplyCommand`, quorum log replication, follower commit & state machine apply loop.
- `TestLeaderFailoverAndReplication`: Leader crash, re-election at higher term, entry preservation, replication on new leader.

Conventions:

- `github.com/stretchr/testify/assert` and `require`
- `go.uber.org/goleak` via `TestMain` — do not leak goroutines or tickers in tests that call `Start`
- Names: `TestServer_<Method>_<Behavior>` or `Test<Feature>`
- Tests mutate `server.currentTerm`, `server.log`, `server.state` directly; keep fields in the same package
- `ApplyCommand` calls must pass a context (`context.Background()` or cancellable test context)

When changing election, replication, or apply: run `go test -race ./raft` or `just test-repeat 20`. CI always uses `-race`.

Preferred next milestones:
1. Leader `commitIndex` calculation from majority `matchIndex` (Raft §5.3 / §5.4).
2. Node restart / rejoin after crash (bringing stopped nodes back online to catch up).
3. Durable persistence (`Storage` interface for `currentTerm`, `votedFor`, and WAL).

## Style

- Standard library + testify + goleak. New deps need a real reason. Do not add OTEL, HTTP servers, or process wrappers.
- Match neighboring files. `gofmt` everything you touch. `server.go` imports are not perfectly grouped; do not reformat the whole file in an unrelated change.
- `log/slog` structured logs (`"server"`, `"term"`, `"Leader"`, `"Candidate"`). Keep that shape.
- Consensus path often logs + returns RPC responses rather than bubbling errors.
- Do not commit `.idea/`. `.gitignore` only lists that directory.

## Working on this repo

1. River writes all code by hand. Agents act as pair reviewers and sounding boards.
2. Discuss design, edge cases, failure modes, and test strategies; do not jump straight to generating implementations.
3. Read `raft/types.go` then the method being discussed in `raft/server.go`.
4. Prefer failing tests first for protocol behavior.
5. Leave persistence / snapshots / membership as explicit follow-ups unless that is the focus.
6. Do not commit or push unless asked.
