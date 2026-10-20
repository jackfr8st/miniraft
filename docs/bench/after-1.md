# MiniRaft benchmark

3-node cluster on localhost · windows/amd64 · 12 CPUs · go1.26.5 · 2026-10-19 19:55

## Write throughput & latency

Each row: fresh cluster, closed-loop `put` clients for 30s, 16-byte values. Latency is per successful `ClientPut` RPC (leader append → majority commit → reply). *Elections* = term increase during the run (0 = one stable leader throughout).

| Clients | Ops | Ops/sec | p50 | p95 | p99 | Max | Errors | Elections | Final log len |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 2062 | 68.7 | 14.27 ms | 16.52 ms | 18.71 ms | 35.27 ms | 4 | 0 | 2063 |
| 8 | 3549 | 118.3 | 61.98 ms | 115.55 ms | 135.76 ms | 163.69 ms | 5 | 0 | 3557 |
| 32 | 3245 | 108.2 | 292.24 ms | 386.47 ms | 426.88 ms | 485.24 ms | 66 | 0 | 3277 |

