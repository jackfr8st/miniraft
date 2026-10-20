# MiniRaft benchmark

3-node cluster on localhost · windows/amd64 · 12 CPUs · go1.26.5 · 2026-10-19 21:06

## Write throughput & latency

Each row: fresh cluster, closed-loop `put` clients for 3s, 16-byte values. Latency is per successful `ClientPut` RPC (leader append → majority commit → reply). *Elections* = term increase during the run (0 = one stable leader throughout).

| Clients | Ops | Ops/sec | p50 | p95 | p99 | Max | Errors | Elections | Final log len |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 229 | 76.3 | 12.92 ms | 14.11 ms | 15.92 ms | 16.76 ms | 2 | 0 | 230 |

## Leader failover

Leader killed with an abrupt process kill (no graceful shutdown). *Elected* = a survivor reports `Leader` at a higher term; *First write* = first `put` that commits. Both are measured from the kill.

| Trial | Killed | New leader | Elected | First write |
|--:|:--|:--|--:|--:|
| 1 | node2 | node3 | 170.99 ms | 185.48 ms |
| 2 | node3 | node1 | 168.62 ms | 181.76 ms |
| 3 | node1 | node3 | 145.07 ms | 159.97 ms |
| 4 | node3 | node2 | 144.37 ms | 158.79 ms |
| 5 | node2 | node1 | 138.33 ms | 148.26 ms |
| 6 | node1 | node3 | 452.75 ms | 466.98 ms |
| 7 | node3 | node1 | 361.77 ms | 375.12 ms |
| 8 | node1 | node2 | 475.45 ms | 487.68 ms |
| 9 | node2 | node3 | 212.94 ms | 227.33 ms |
| 10 | node3 | node1 | 168.32 ms | 180.63 ms |
| 11 | node1 | node2 | 177.14 ms | 186.85 ms |
| 12 | node2 | node1 | 381.45 ms | 391.37 ms |
| 13 | node1 | node3 | 141.06 ms | 155.12 ms |
| 14 | node3 | node1 | 158.91 ms | 174.38 ms |
| 15 | node1 | node3 | 219.35 ms | 234.44 ms |
| **min** | | | 138.33 ms | 148.26 ms |
| **median** | | | 170.99 ms | 185.48 ms |
| **max** | | | 475.45 ms | 487.68 ms |

