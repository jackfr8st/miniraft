# MiniRaft benchmark

3-node cluster on localhost · windows/amd64 · 12 CPUs · go1.26.5 · 2026-10-19 19:26

## Write throughput & latency

Each row: fresh cluster, closed-loop `put` clients for 30s, 16-byte values. Latency is per successful `ClientPut` RPC (leader append → majority commit → reply). *Elections* = term increase during the run (0 = one stable leader throughout).

| Clients | Ops | Ops/sec | p50 | p95 | p99 | Max | Errors | Elections | Final log len |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 2058 | 68.6 | 14.23 ms | 16.64 ms | 18.34 ms | 50.95 ms | 0 | 0 | 2059 |
| 8 | 3717 | 123.9 | 58.47 ms | 106.26 ms | 130.07 ms | 166.21 ms | 5 | 0 | 3725 |
| 32 | 1035 | 34.5 | 203.10 ms | 276.45 ms | 311.50 ms | 324.60 ms | 65138 | 112 | 1071 |

