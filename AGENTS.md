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
  types.go              # Term, LogEntry, ServerState, ClusterMember, Command, StateMachine, Transport
  state_machine.go      # NoOpStateMachine (tests)
  server_test.go        # unit tests (RequestVote, AppendEntries, ApplyCommand)
kv/sm.go                # in-memory KV StateMachine + binary command encoding
.github/workflows/ci.yml
```

Packages:

- `raft` — consensus. Depend on this.
- `kv` — optional `raft.StateMachine` used as a real SM in tests. Not a service.

Do not invent packages. Do not reintroduce `main`, Docker, OTEL, or a demo loop unless that is the task. There is no client library, no membership-change RPC, and no persistence layer.

## What exists vs what does not

Implemented:

- Follower / Candidate / Leader
- RequestVote and AppendEntries over injected `Transport` interface
- Randomized election timeout, heartbeats
- Log append + conflict truncate on followers
- Leader `ApplyCommand(ctx, cmds...)` + parallel AppendEntries to peers
- Follower redirect via `NotLeaderError` (LeaderId)
- KV get/set encoding; apply happens on the leader after a quorum wait
- `NewServer(id, sm, members, transport, opts...)` functional options

Not implemented (do not pretend they work):

- Persistent `currentTerm` / `votedFor` / log (`Todo: Load from disk` / `persist to disk`)
- Follower apply loop (`lastApplied` is only bumped on the leader in `ApplyCommand`)
- Correct commit-index advancement from matchIndex (leader currently bumps `commitIndex` locally when appending)
- Snapshotting, log compaction, membership changes
- Bounded AppendEntries batches / backoff on retry
- Multi-node / in-process cluster tests

## Commands

```bash
go test ./...
go test -race -coverprofile=coverage.txt -covermode=atomic ./...   # CI
go test -race -count=1 ./raft
```

IDs must be >= 1. Cluster size must be odd (`NewServer` panics otherwise).

## RPC / Transport

RPCs are defined as Go struct types and handled via the `Transport` interface (`raft/types.go`):

- `AppendEntries(ctx context.Context, to MemberId, req *AppendEntriesRequest) (*AppendEntriesResult, error)`
- `RequestVote(ctx context.Context, to MemberId, req *RequestVoteRequest) (*RequestVoteResult, error)`

`Server` handles incoming RPCs via direct method calls (`AppendEntries`, `RequestVote`) and sends outgoing RPCs via its injected `Transport`.

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

Known sharp edges (fix with tests, don't paper over):

- `ApplyCommand` waitgroup is `Add(quorum)` then `Done()` once per *other* member — mismatch on cluster size != 3.
- `requestVoteFromMember` uses `s.log[logLen-1]` after `logLen := len(s.log)-1` (off-by-one vs last entry).
- `sendHeartbeat` is sometimes called with `nil` context from the ticker.
- `AppendEntries` panics if `s.state != Follower` after `updateTerm` (updateTerm only steps down when `term > currentTerm`).
- Replication retry loop can dereference `connResp` after a non-nil `err`.
- `Start` allocates an unused vote `WaitGroup` (Add without Done/Wait).

## Tests

All tests are in `raft/server_test.go`. They are mostly single-node RPC unit tests against a `NoOpStateMachine` and three fake members (`one`/`two`/`three`). There is no multi-node integration test and no `kv` test file.

Conventions:

- `github.com/stretchr/testify/assert` and `require`
- `go.uber.org/goleak` via `TestMain` — do not leak goroutines or tickers in tests that call `Start`
- Names: `TestServer_<Method>_<Behavior>`
- Tests mutate `server.currentTerm`, `server.log`, `server.state` directly; keep fields in the same package
- `ApplyCommand` calls must pass a context (`context.Background()` is fine in unit tests)

When changing election, replication, or apply: add a focused test in `server_test.go` and run `go test -race ./raft`. CI always uses `-race`.

Preferred next test (do this instead of plumbing): an in-process 3-node cluster with a fake transport and a controllable clock. One failing test that defines "core is down": elect a leader → `ApplyCommand` → every log identical and every SM applied → kill leader → new leader still has the committed entry.

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
