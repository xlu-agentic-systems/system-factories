# PostgreSQL Benchmark Results

This run used a real local PostgreSQL 16 database, not an in-memory simulator.

The same benchmark code ran two workloads:

- `hot`: 10,000 unique users all like one post, causing 10,000 increments of the same counter row.
- `distributed`: 10,000 unique users like 1,000 posts, causing roughly 10 increments per counter row.

## Command

```bash
python3 scripts/run_benchmark.py \
  --operations 10000 \
  --posts 1000 \
  --workers 128 \
  --mode all \
  --output results/10k.json
```

## Result

| mode | operations | posts | workers | successful writes | ops/sec | avg ms | p50 ms | p95 ms | p99 ms | hottest post |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| hot | 10000 | 1000 | 128 | 10000 | 48.4 | 2639.60 | 1669.08 | 8675.34 | 13402.52 | 10000 |
| distributed | 10000 | 1000 | 128 | 10000 | 1359.7 | 93.60 | 80.54 | 187.12 | 368.45 | 10 |

## Interpretation

The hot workload is much slower because all successful likes increment the same `post_counters` row. PostgreSQL must serialize those updates through the row lock for that counter.

The distributed workload writes the same number of logical likes to the database, but the counter updates are spread across 1,000 `post_counters` rows. That lets PostgreSQL execute many independent row updates in parallel.

This benchmark demonstrates a real single-node relational hot-row bottleneck:
even before a distributed database hot-partition problem appears, one popular
counter row can become the write serialization point.

## Practical Takeaway

For a viral post, a single exact counter row is a bad write path. Common production mitigations include sharded counters, append-only like events with asynchronous aggregation, or approximate cached display counts backed by exact idempotent like state.
