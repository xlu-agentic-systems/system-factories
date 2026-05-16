# Experiment Details

This project benchmarks two 8-character URL short-code strategies:

```text
base62: clean URL -> SHA-256 -> Base62 -> first 8 chars
base36: clean URL -> SHA-256 -> Base36 -> first 8 chars
```

Both strategies use the same generated long URL stream and write to PostgreSQL tables with:

```sql
short_url VARCHAR(8) PRIMARY KEY
long_url TEXT NOT NULL UNIQUE
```

## URL Cleaning

Cleaning is intentionally basic and deterministic:

- Trim whitespace.
- Add `https://` when no scheme exists.
- Lowercase scheme and host.
- Remove default ports.
- Collapse repeated slashes in the path.
- Remove fragments.
- Remove common tracking parameters such as `utm_source`, `utm_medium`, `gclid`, and `fbclid`.
- Sort remaining query parameters.

The generated benchmark URLs preserve a non-tracking `v` parameter, so all generated URLs remain distinct after cleaning.

## Collision Handling

For every long URL:

1. Generate `short_url` with retry attempt `0`.
2. Insert into PostgreSQL.
3. If `short_url` already exists for a different `long_url`, count a collision.
4. Retry with salted hash material: `canonical_url + "\\0retry=N"`.
5. Stop after `max_retries=3`.
6. If every retry collides, insert a row into `collision_failures`.

The benchmark batches work through a temporary staging table:

```text
stage_url_mappings -> INSERT INTO url_mappings_* ON CONFLICT DO NOTHING
```

After the insert, it joins staging rows back to the target table to find only the rows whose `short_url` mapped to a different `long_url`; those rows are retried.

## 100M Collision Math

For 100,000,000 distinct long URLs, expected collision pairs are approximately:

```text
base62 code space = 62^8 = 218,340,105,584,896
base62 expected collision pairs = 22.90

base36 code space = 36^8 = 2,821,109,907,456
base36 expected collision pairs = 1,772.35
```

This is only the birthday-bound expectation. The PostgreSQL benchmark is the exact check for the deterministic generated URL set.

## Commands

Start PostgreSQL:

```bash
docker compose up -d
```

Run the 100M benchmark against both methods:

```bash
python scripts/benchmark.py \
  --backend postgres \
  --reset \
  --total 100000000 \
  --chunk-size 1000000 \
  --batch-size 50000 \
  --max-retries 3
```

Print expected collision math only:

```bash
python scripts/benchmark.py --backend math --total 100000000
```

## Verified Smoke Run

Local PostgreSQL smoke run:

```text
total=100,000
chunk_size=50,000
batch_size=10,000
```

Results:

```text
base62:
  inserted=100,000
  collisions=0
  retry_successes=0
  failures_after_retries=0
  elapsed_seconds=2.52
  throughput_urls_per_sec=39,620

base36:
  inserted=100,000
  collisions=0
  retry_successes=0
  failures_after_retries=0
  elapsed_seconds=2.61
  throughput_urls_per_sec=38,286
```

No collisions are expected at 100k for either method. The purpose of this smoke run is to verify the PostgreSQL schema, staging insert path, and CLI.

