# Benchmark Results

Local benchmark run with PostgreSQL and PgBouncer in Docker.

Environment:

```text
PostgreSQL: 127.0.0.1:25432
PgBouncer: 127.0.0.1:26432
rows=10,000
query=SELECT payload FROM benchmark_items WHERE id = $1
pool_size=32 for low/mid QPS
pool_size=128 for high QPS
```

## Low and Mid QPS Sweep

Command:

```bash
python scripts/benchmark.py \
  --qps 100 500 1000 2500 \
  --duration 2 \
  --pool-size 32 \
  --max-in-flight 5000
```

Results:

```text
mode,target_qps,scheduled,completed,errors,elapsed_seconds,achieved_qps,p50_ms,p95_ms,p99_ms,max_ms
direct_new,100,200,200,0,2.000,100.0,4.099,5.460,6.681,11.206
direct_pool,100,200,200,0,2.012,99.4,1.291,2.034,2.125,2.625
pgbouncer_new,100,200,200,0,2.000,100.0,3.329,4.322,4.765,11.423
pgbouncer_pool,100,200,200,0,2.006,99.7,2.121,3.042,3.432,5.276

direct_new,500,1000,1000,0,2.002,499.5,2.939,3.707,4.083,6.550
direct_pool,500,1000,1000,0,2.011,497.3,1.096,1.718,2.657,6.298
pgbouncer_new,500,1000,1000,0,2.000,499.9,1.563,2.016,2.713,7.266
pgbouncer_pool,500,1000,1000,0,2.004,498.9,1.145,1.726,2.216,4.676

direct_new,1000,2000,2000,0,2.001,999.3,2.949,3.457,6.875,15.089
direct_pool,1000,2000,2000,0,2.010,994.9,0.564,0.991,1.372,3.371
pgbouncer_new,1000,2000,2000,0,2.002,999.0,1.584,1.711,2.650,3.555
pgbouncer_pool,1000,2000,2000,0,2.001,999.3,0.570,0.927,1.722,2.587

direct_new,2500,5000,5000,0,2.006,2492.5,4.970,10.995,12.538,14.271
direct_pool,2500,5000,5000,0,2.004,2495.3,0.337,0.363,0.470,1.472
pgbouncer_new,2500,5000,5000,0,2.002,2497.2,2.994,4.377,7.777,11.984
pgbouncer_pool,2500,5000,5000,0,2.001,2498.5,0.431,0.466,0.537,2.531
```

Key result: at 2,500 QPS, app-side pooling reduced p95 from `10.995ms` in `direct_new` to `0.363ms` in `direct_pool`, about `30x` lower p95 latency.

## High-QPS Simulation

Command:

```bash
python scripts/benchmark.py \
  --qps 10000 25000 50000 100000 \
  --duration 2 \
  --pool-size 128 \
  --max-in-flight 20000 \
  --modes direct_pool pgbouncer_pool
```

Results:

```text
mode,target_qps,scheduled,completed,errors,elapsed_seconds,achieved_qps,p50_ms,p95_ms,p99_ms,max_ms
direct_pool,10000,20000,20000,0,2.011,9944.6,0.424,0.503,0.601,2.102
pgbouncer_pool,10000,20000,20000,0,2.003,9983.7,0.556,0.682,1.157,5.804

direct_pool,25000,49995,49995,0,2.013,24835.0,1.336,4.378,7.485,13.489
pgbouncer_pool,25000,49997,49997,0,2.003,24955.0,1.720,7.731,9.963,14.220

direct_pool,50000,99999,99999,0,4.318,23159.2,545.192,2516.139,3325.019,3883.206
pgbouncer_pool,50000,99932,99932,0,4.350,22974.1,615.954,2554.675,3390.283,3926.003

direct_pool,100000,199884,199884,0,8.991,22231.2,9.777,3770.438,5314.533,8725.955
pgbouncer_pool,100000,199929,199929,0,8.829,22644.0,92.051,3209.580,4886.391,8578.540
```

Interpretation:

- The local stack sustains roughly `25k QPS` with acceptable latency for this tiny query.
- At requested `50k` and `100k QPS`, achieved QPS stops increasing and tail latency grows into seconds. That is saturation, not a successful 100k-QPS serving result.
- Per-request connection creation should not be used for high QPS. Even at 2,500 QPS, pooling is already dramatically lower latency.
- PgBouncer is most useful when many app instances would otherwise create too many database server connections. In this single-process local benchmark, direct app pooling can be slightly faster because it avoids an extra proxy hop.

