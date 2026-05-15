# Benchmark Results

These results were run locally against DynamoDB Local on May 15, 2026.

Command:

```bash
python3 scripts/run_benchmark.py --operations 10000 --posts 1000 --workers 128 --mode all --output results/10k.json
```

Result:

```text
mode         ops      posts  workers  ok       conflicts  ops/s    avg_ms  p95_ms   p99_ms   hottest_post
-----------  -------  -----  -------  -------  ---------  -------  ------  -------  -------  ------------
hot            10000   1000      128    10000          0    168.8  645.39  1300.90  1760.47         10000
distributed    10000   1000      128    10000          0    186.7  530.36  1251.79  1791.12            10
```

## Interpretation

The hot workload sends all likes to one post:

```text
PostCounters[post-000001] += 1
```

The distributed workload spreads the same number of likes across 1,000 posts:

```text
PostCounters[post-000001] += 1
PostCounters[post-000002] += 1
...
```

DynamoDB Local is a single local process, so it does not perfectly model AWS DynamoDB partition-level scaling. The relative shape still shows the intended pressure:

- hot post: one counter item absorbs 10,000 updates
- distributed: hottest post absorbs only 10 updates
- hot post has lower throughput and higher average latency in this run

At 100 million DAU, the hot-post case is the dangerous one. A viral post can concentrate millions of writes on one logical counter item. The production fix is usually not to keep updating one counter row forever, but to shard or aggregate the counter.

## Next Benchmark Variants

Useful follow-ups:

- Sharded counters: `post_id#shard_id`.
- Stream aggregation: write like events and aggregate asynchronously.
- Transactional write path: exactly-once like row plus counter update.
- Duplicate/retry workloads using idempotency.
- Unlike-heavy workloads.

## Row-Lock Simulator Result

Command:

```bash
python3 scripts/run_lock_sim.py --operations 10000 --posts 1000 --workers 128 --update-ms 1 --mode all
```

Result:

```text
mode         ops      posts  workers  update_ms  ops/s     avg_ms  p95_ms   p99_ms   hottest_post
-----------  -------  -----  -------  ---------  --------  ------  -------  -------  ------------
hot            10000   1000      128       1.00     775.9  163.79   652.85   980.20         10000
distributed    10000   1000      128       1.00   59947.4    1.84     6.60    13.21            10
```

This makes the intended contention effect visible. The hot workload is bounded by one lock, while the distributed workload can run across many independent locks.
