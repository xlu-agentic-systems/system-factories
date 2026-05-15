# PostgreSQL Like Hot-Row Benchmark

This project redoes the like-counter hot-key experiment against a real local PostgreSQL database.
It is intentionally not a Python lock simulator and not DynamoDB Local.

The goal is to show what happens when many concurrent requests perform true database writes against:

- one hot counter row for a popular post
- many independent counter rows for distributed posts

It compares:

```text
hot:
  many unique users like one popular post

distributed:
  the same number of unique users are spread across many posts
```

Unlike the Python lock simulator, this benchmark performs true database writes:

```sql
INSERT INTO likes (user_id, post_id)
VALUES ($1, $2)
ON CONFLICT DO NOTHING;

UPDATE post_counters
SET like_count = like_count + 1
WHERE post_id = $1;
```

The hot case updates the same `post_counters` row repeatedly, so PostgreSQL serializes those updates through the row lock. The distributed case updates many different `post_counters` rows, so PostgreSQL can run more updates in parallel.

That is the point of the benchmark: the total number of writes is the same, but the lock target is different.

## Run

Start PostgreSQL:

```bash
docker compose up -d
```

Install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run tests:

```bash
pytest -q
```

Run the benchmark:

```bash
python3 scripts/run_benchmark.py --operations 10000 --posts 1000 --workers 128 --mode all --output results/10k.json
```

See [docs/benchmark-results.md](docs/benchmark-results.md) for one local run.

## What The Result Means

In the local 10k run, the hot-row workload completed at roughly `48 ops/sec`, while the distributed-row workload completed at roughly `1,360 ops/sec`.

Both modes insert into `likes` and increment counters. The difference is that hot mode repeatedly increments:

```text
post_counters[post-000001]
```

while distributed mode spreads the increments across:

```text
post_counters[post-000001]
post_counters[post-000002]
...
post_counters[post-001000]
```

So the result is not caused by fewer writes in the distributed case. It is caused by less contention on a single database row.

## Expected Shape

The hot workload should have much lower throughput and much higher latency because every counter update locks the same row:

```text
post_counters[post-000001]
```

The distributed workload spreads lock acquisition across many rows:

```text
post_counters[post-000001]
post_counters[post-000002]
...
```

This is the single-node relational version of the hot-key problem.
