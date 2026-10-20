# MiniRaft

A from-scratch implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf)
in Go — a fault-tolerant, replicated key-value store built as a distributed
systems portfolio project. Verified via real process kills and automated
chaos testing, not simulation.


- **Leader election** — randomized election timeouts, majority-vote
  leadership, heartbeats to maintain leadership and prevent split votes,
  and the election restriction (Raft section 5.4.1): a node votes only for
  a candidate whose log is at least as up to date as its own, so every
  elected leader holds all committed entries.
- **Log replication** — `AppendEntries` carrying real log entries,
  `nextIndex`/`matchIndex` reconciliation, follower log-consistency checks
  and conflict resolution, majority-plus-current-term commit safety rule.
- **Persistence** — `currentTerm`, `votedFor`, and the log survive a
  process restart: state is written to a temp file, fsynced, then swapped
  into place.
- **Observability & chaos testing** — a `DebugState` RPC for on-demand
  cluster introspection, and an automated Go-native chaos-testing harness
  (`cmd/chaos`) that writes continuously while killing random nodes on a
  timer and asserts zero data loss.

**Verified result:** a value written through one leader, which is then
killed mid-run, is correctly read back from a different node under a later
term — real leader failure, zero data loss, not simulated. The automated
chaos harness ran 57 writes through 5 kill/restart cycles rotating across
all three nodes with zero failures.

## Running locally

Requires Go 1.26.5+ (per `go.mod`).

```bash
go build -o bin/miniraft ./cmd/miniraft
go build -o bin/client ./cmd/client
go build -o bin/debug ./cmd/debug
go build -o bin/chaos ./cmd/chaos
```

Start a 3-node cluster, each in its own terminal:

```bash
./bin/miniraft -id=node1 -addr=:9001 -peers="node2=127.0.0.1:9002,node3=127.0.0.1:9003"
./bin/miniraft -id=node2 -addr=:9002 -peers="node1=127.0.0.1:9001,node3=127.0.0.1:9003"
./bin/miniraft -id=node3 -addr=:9003 -peers="node1=127.0.0.1:9001,node2=127.0.0.1:9002"
```

Write and read through any node — the client follows "not leader"
redirects automatically:

```bash
./bin/client -addr=127.0.0.1:9001 -op=put -key=foo -value=bar
./bin/client -addr=127.0.0.1:9001 -op=get -key=foo
```

Inspect any node's internal state on demand:

```bash
./bin/debug -addr=127.0.0.1:9001
# {State:Leader Term:4 LogLen:1 CommitIndex:1 LastApplied:1 WhoLeader:node1}
```

Kill the leader's process (Ctrl-C or `kill`) and watch the remaining nodes
elect a new one within roughly one election timeout (150-300ms) - the
core Raft safety/liveness guarantee, live.

### Automated chaos test

```bash
./bin/chaos -duration=30s -killInterval=5s
```

Starts a fresh 3-node cluster, writes continuously, verifies a random
sample of prior writes every cycle, and kills/restarts a random node every
`killInterval`. Exits non-zero and prints every mismatch if any committed
write is ever lost or returns a stale value.

### Benchmark

```bash
go run ./cmd/bench -out=bench.md
```

Builds the node binary, then measures leader failover (kills the leader 5
times; time until a new leader is elected and until the next write commits)
and write throughput with 1, 8 and 32 concurrent clients for 30s each, each
level on a fresh cluster. Prints ops/sec and p50/p95/p99 latency as a
markdown table. Nodes listen on 9101-9103 so a dev cluster on 9001 is
unaffected. See `-h` for flags (`-duration`, `-clients`, `-failoverTrials`,
`-keep` to retain node logs).

#### Results: per-peer connection reuse at 32 clients

Medians of 3 runs each, 30s per run, on a 3-node localhost cluster
(windows/amd64, 12 CPUs, go1.26.5):

| 32 clients | Ops/sec | Errors | Elections |
|:--|--:|--:|--:|
| Before | 31.7 | 65,138 | 112 |
| After  | 109.2 | 66 | 0 |

Root cause: every RPC dialed a fresh TCP connection, leaving a `TIME_WAIT`
socket behind each call, so under load the OS ran out of ephemeral ports,
heartbeats failed, and followers kept starting elections. The fix keeps one
persistent `net/rpc` connection per peer (`raft/server.go`). Raw reports for
every run, including leader-failover timings, are in
[`docs/bench/`](./docs/bench/).

## Project structure

```
raft/
  node.go       Core state machine: election, heartbeats, AppendEntries handling
  rpc.go        RPC message types (RequestVote, AppendEntries, ClientPut/Get, DebugState)
  server.go     net/rpc server wiring and the client-side callRPC helper
                (one persistent connection per peer)
  client.go     ClientPut/ClientGet handlers, leader-redirect logic, commit-wait
  persist.go    Save (temp file, fsync, swap into place) and load of persistent state
  persist_test.go   Unit tests for the persistence layer
cmd/
  miniraft/     The node executable
  client/       CLI client for put/get against the cluster
  debug/        CLI tool for querying DebugState
  chaos/        Automated chaos-testing harness
  bench/        Throughput/latency and leader-failover benchmark
docs/
  bench/        Benchmark reports (before/after connection reuse, failover)
chaos_test.ps1  Original PowerShell chaos-test driver, superseded by
                cmd/chaos — kept for reference
```

## Design decisions and trade-offs

The main design decisions, the alternatives they were weighed against, and
the notable bugs found along the way:

- **`net/rpc` over gRPC** - standard library, zero dependencies, exposes
  the RPC mechanics directly; trades away cross-language clients and
  schema versioning that a real production system would want.
- **JSON persistence over an embedded store (e.g. BoltDB)** - human-readable
  and simple to debug at this scale; trades away the incremental-append
  performance a production system with a large log would need.
- **A leader can only directly commit log entries from its own current
  term** (Raft paper section 5.4.2) - the single trickiest safety rule in
  the algorithm; entries from earlier terms are committed only indirectly,
  once an entry from the current term reaches a majority.
- **A real, found-and-fixed cross-platform bug:** on Windows, renaming
  the temp file over an existing state file intermittently failed under
  load, so state saves failed - diagnosed from the exact OS error string.
  The workaround removes the old state file first and then renames the
  new one into place, which trades away crash safety during the swap (see
  Known limitations).
- **The chaos-testing harness was rewritten from PowerShell to native Go**
  after an extended debugging process revealed the failures were entirely
  in shell scripting/tooling (quoting, multi-line output parsing), never
  in the Raft implementation itself - a real example of abandoning a
  struggling approach for a more direct one once the evidence supported it.

## Known limitations / not implemented

- No graceful shutdown signal handler (nodes are stopped via kill signal).
- No conflict-index optimization on `AppendEntries` rejection - `nextIndex`
  backs off one entry at a time rather than jumping to the follower's
  actual point of divergence (correct, just slower to converge after a
  leader change touches many entries).
- No log compaction / snapshotting - the log grows unboundedly.
- Reads are served only by the leader, straight from applied state;
  no read-index/lease-read optimization for linearizable follower reads.
- State replacement removes the old file before renaming the new one, so a
  crash between the two calls can lose persisted state.

## Reference

- Ongaro, Diego, and John Ousterhout. **"In Search of an Understandable
  Consensus Algorithm (Extended Version)."** *Proceedings of the 2014
  USENIX Annual Technical Conference (USENIX ATC '14)*, 2014.
  [raft.github.io/raft.pdf](https://raft.github.io/raft.pdf) - the paper
  this implementation follows. Section references in this README (e.g.
  section 5.4.2 commit safety) refer to this paper.
- [The Raft Consensus Algorithm website](https://raft.github.io/) - the
  paper's companion site, including the interactive visualization used
  to sanity-check election and replication behavior during development.

This is a learning/portfolio implementation of a well-established,
published algorithm - Raft was explicitly designed to be more
understandable and implementable than Paxos, and implementing it from the
paper is a standard distributed-systems exercise (e.g. MIT's 6.824/6.5840
course). No part of the consensus algorithm itself is original; the
value of this project is in the from-scratch Go implementation, the
verified correctness testing, and the benchmarking and debugging work
behind it.
