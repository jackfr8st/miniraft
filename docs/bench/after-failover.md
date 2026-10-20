# MiniRaft benchmark

3-node cluster on localhost · windows/amd64 · 12 CPUs · go1.26.5 · 2026-10-19 20:10

## Write throughput & latency

Each row: fresh cluster, closed-loop `put` clients for 5s, 16-byte values. Latency is per successful `ClientPut` RPC (leader append → majority commit → reply). *Elections* = term increase during the run (0 = one stable leader throughout).

| Clients | Ops | Ops/sec | p50 | p95 | p99 | Max | Errors | Elections | Final log len |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 379 | 75.8 | 13.04 ms | 14.03 ms | 14.60 ms | 15.57 ms | 3 | 0 | 380 |

## Leader failover

Leader killed with an abrupt process kill (no graceful shutdown). *Elected* = a survivor reports `Leader` at a higher term; *First write* = first `put` that commits. Both are measured from the kill.

| Trial | Killed | New leader | Elected | First write |
|--:|:--|:--|--:|--:|
| 1 | node1 | node2 | 160.76 ms | 174.00 ms |
| 2 | node2 | node3 | 257.32 ms | 273.48 ms |
| 3 | node3 | node1 | 148.05 ms | 163.93 ms |
| 4 | node1 | node2 | 161.03 ms | 175.22 ms |
| 5 | node2 | node3 | 177.82 ms | 191.68 ms |
| **min** | | | 148.05 ms | 163.93 ms |
| **median** | | | 161.03 ms | 175.22 ms |
| **max** | | | 257.32 ms | 273.48 ms |

