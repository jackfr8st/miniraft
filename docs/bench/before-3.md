# MiniRaft benchmark

3-node cluster on localhost · windows/amd64 · 12 CPUs · go1.26.5 · 2026-10-19 19:35

## Write throughput & latency

Each row: fresh cluster, closed-loop `put` clients for 30s, 16-byte values. Latency is per successful `ClientPut` RPC (leader append → majority commit → reply). *Elections* = term increase during the run (0 = one stable leader throughout).

| Clients | Ops | Ops/sec | p50 | p95 | p99 | Max | Errors | Elections | Final log len |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 2106 | 70.2 | 14.14 ms | 15.32 ms | 17.36 ms | 29.88 ms | 0 | 0 | 2107 |
| 8 | 4075 | 135.8 | 57.28 ms | 91.67 ms | 103.50 ms | 129.21 ms | 5 | 0 | 4083 |
| 32 | 625 | 20.8 | 188.33 ms | 237.06 ms | 306.14 ms | 307.25 ms | 54486 | 115 | no leader |

