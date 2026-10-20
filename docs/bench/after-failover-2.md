# MiniRaft benchmark

3-node cluster on localhost · windows/amd64 · 12 CPUs · go1.26.5 · 2026-10-19 21:05

## Write throughput & latency

Each row: fresh cluster, closed-loop `put` clients for 5s, 16-byte values. Latency is per successful `ClientPut` RPC (leader append → majority commit → reply). *Elections* = term increase during the run (0 = one stable leader throughout).

| Clients | Ops | Ops/sec | p50 | p95 | p99 | Max | Errors | Elections | Final log len |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 359 | 71.8 | 13.58 ms | 15.28 ms | 19.15 ms | 37.72 ms | 2 | 0 | 360 |

## Leader failover

Leader killed with an abrupt process kill (no graceful shutdown). *Elected* = a survivor reports `Leader` at a higher term; *First write* = first `put` that commits. Both are measured from the kill.

| Trial | Killed | New leader | Elected | First write |
|--:|:--|:--|--:|--:|
| 1 | node3 | node2 | 165.32 ms | 180.15 ms |
| 2 | node2 | node3 | 422.62 ms | 437.23 ms |
| 3 | node3 | node1 | 175.27 ms | 191.10 ms |
| 4 | node1 | node2 | 259.83 ms | 269.73 ms |
| 5 | node2 | node1 | 353.44 ms | 366.79 ms |
| **min** | | | 165.32 ms | 180.15 ms |
| **median** | | | 259.83 ms | 269.73 ms |
| **max** | | | 422.62 ms | 437.23 ms |

