# MiniRaft benchmark

3-node cluster on localhost · windows/amd64 · 12 CPUs · go1.26.5 · 2026-10-19 19:58

## Write throughput & latency

Each row: fresh cluster, closed-loop `put` clients for 30s, 16-byte values. Latency is per successful `ClientPut` RPC (leader append → majority commit → reply). *Elections* = term increase during the run (0 = one stable leader throughout).

| Clients | Ops | Ops/sec | p50 | p95 | p99 | Max | Errors | Elections | Final log len |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 1962 | 65.4 | 15.02 ms | 17.07 ms | 18.55 ms | 45.16 ms | 3 | 0 | 1963 |
| 8 | 3576 | 119.2 | 63.65 ms | 104.86 ms | 120.97 ms | 175.58 ms | 6 | 0 | 3584 |
| 32 | 3277 | 109.2 | 287.81 ms | 400.86 ms | 437.01 ms | 592.07 ms | 65 | 0 | 3309 |

