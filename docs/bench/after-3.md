# MiniRaft benchmark

3-node cluster on localhost · windows/amd64 · 12 CPUs · go1.26.5 · 2026-10-19 20:02

## Write throughput & latency

Each row: fresh cluster, closed-loop `put` clients for 30s, 16-byte values. Latency is per successful `ClientPut` RPC (leader append → majority commit → reply). *Elections* = term increase during the run (0 = one stable leader throughout).

| Clients | Ops | Ops/sec | p50 | p95 | p99 | Max | Errors | Elections | Final log len |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 1969 | 65.6 | 15.01 ms | 17.08 ms | 18.59 ms | 35.91 ms | 0 | 0 | 1970 |
| 8 | 3744 | 124.8 | 61.53 ms | 105.10 ms | 117.96 ms | 131.89 ms | 6 | 0 | 3752 |
| 32 | 3321 | 110.7 | 282.97 ms | 396.17 ms | 423.13 ms | 468.39 ms | 78 | 0 | 3353 |

