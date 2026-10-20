# MiniRaft benchmark

3-node cluster on localhost · windows/amd64 · 12 CPUs · go1.26.5 · 2026-10-19 21:06

## Write throughput & latency

Each row: fresh cluster, closed-loop `put` clients for 3s, 16-byte values. Latency is per successful `ClientPut` RPC (leader append → majority commit → reply). *Elections* = term increase during the run (0 = one stable leader throughout).

| Clients | Ops | Ops/sec | p50 | p95 | p99 | Max | Errors | Elections | Final log len |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 232 | 77.3 | 12.89 ms | 13.61 ms | 14.41 ms | 14.64 ms | 0 | 0 | 233 |

## Leader failover

Leader killed with an abrupt process kill (no graceful shutdown). *Elected* = a survivor reports `Leader` at a higher term; *First write* = first `put` that commits. Both are measured from the kill.

| Trial | Killed | New leader | Elected | First write |
|--:|:--|:--|--:|--:|
| 1 | node2 | node3 | 237.13 ms | 252.91 ms |
| 2 | node3 | node1 | 1270.30 ms | 1278.88 ms |
| 3 | node1 | node3 | 141.43 ms | 157.48 ms |
| 4 | node3 | node2 | 333.93 ms | 348.57 ms |
| 5 | node2 | node3 | 173.95 ms | 189.44 ms |
| 6 | node3 | node1 | 436.16 ms | 447.72 ms |
| 7 | node1 | node2 | 150.10 ms | 162.72 ms |
| 8 | node2 | node1 | 207.81 ms | 217.45 ms |
| 9 | node1 | node3 | 233.32 ms | 247.64 ms |
| 10 | node3 | node2 | 145.09 ms | 158.10 ms |
| 11 | node2 | node1 | 137.47 ms | 150.23 ms |
| 12 | node1 | node2 | 218.63 ms | 231.66 ms |
| 13 | node2 | node3 | 160.39 ms | 173.84 ms |
| 14 | node3 | node1 | 160.09 ms | 172.33 ms |
| 15 | node1 | node3 | 227.52 ms | 243.16 ms |
| **min** | | | 137.47 ms | 150.23 ms |
| **median** | | | 207.81 ms | 217.45 ms |
| **max** | | | 1270.30 ms | 1278.88 ms |

