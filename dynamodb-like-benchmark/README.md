# DynamoDB Like Benchmark

This project benchmarks a simple like/unlike service on DynamoDB Local.

The core write path is intentionally direct:

```text
1. Put one user/post like row for idempotency.
2. Update one post counter row with ADD like_count :one.
```

The benchmark compares two cases:

```text
hot:
  many users like the same popular post

distributed:
  the same number of users are spread across many posts
```

This demonstrates the hot-key problem. Updating one counter item at a time is simple, but a popular post concentrates writes onto one DynamoDB item.

See [docs/benchmark-results.md](docs/benchmark-results.md) for one local DynamoDB Local run.

## Schema

`Likes` records idempotent user likes:

```text
PK user_id
SK post_id
```

`PostCounters` stores one counter item per post:

```text
PK post_id
like_count
```

The counter update is:

```text
UpdateItem PostCounters
ADD like_count :one
WHERE post_id = :post_id
```

## Run

Start DynamoDB Local:

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

Run a local benchmark:

```bash
python3 scripts/run_benchmark.py --operations 10000 --posts 1000 --workers 128 --mode all
```

Run a larger local benchmark:

```bash
python3 scripts/run_benchmark.py --operations 100000 --posts 10000 --workers 256 --mode all --output results/100k.json
```

## Why This Matters

At 100 million DAU, a normal post may be fine with direct counter updates. A globally popular post is different: millions of users can attempt to like the same item in a short window.

If every like directly updates:

```text
PostCounters[post_id = viral-post]
```

then the post counter item becomes the serialized bottleneck.

Distributed traffic behaves better because writes spread across many partition keys:

```text
PostCounters[post-1]
PostCounters[post-2]
...
PostCounters[post-N]
```

## Production Direction

The direct counter design is useful as a baseline. For a production-scale like system, common next steps are:

- sharded counters per post
- asynchronous aggregation from a stream
- write coalescing for viral posts
- approximate counters for display
- exact per-user like state for idempotency
- delayed reconciliation for final counts
