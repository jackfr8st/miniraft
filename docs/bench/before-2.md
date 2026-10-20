# MiniRaft benchmark

3-node cluster on localhost · windows/amd64 · 12 CPUs · go1.26.5 · 2026-10-19 19:31

## Write throughput & latency

Each row: fresh cluster, closed-loop `put` clients for 30s, 16-byte values. Latency is per successful `ClientPut` RPC (leader append → majority commit → reply). *Elections* = term increase during the run (0 = one stable leader throughout).

| Clients | Ops | Ops/sec | p50 | p95 | p99 | Max | Errors | Elections | Final log len |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 2026 | 67.5 | 14.60 ms | 16.37 ms | 18.86 ms | 37.46 ms | 3 | 0 | 2027 |
| 8 | 3759 | 125.3 | 61.59 ms | 102.97 ms | 111.21 ms | 127.04 ms | 6 | 0 | 3767 |
| 32 | 951 | 31.7 | 222.62 ms | 294.01 ms | 349.45 ms | 359.06 ms | 70343 | 103 | no leader |

