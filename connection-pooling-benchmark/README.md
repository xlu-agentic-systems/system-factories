# Connection Pooling Benchmark

Benchmark the performance difference between opening a fresh PostgreSQL connection per request and reusing connections through application-side pooling and PgBouncer.

## Why PostgreSQL

PostgreSQL is the right target for this benchmark because every client connection maps to a backend server process. Creating and tearing down those connections is meaningfully expensive, and too many concurrent connections can pressure the database.

SQLite is not a good target because it is embedded/serverless; there is no database server connection to pool. Redis is useful for connection pooling experiments, but Redis high-QPS improvements are often dominated by pipelining or multiplexing rather than classic database connection pooling.

## Benchmark Modes

```text
direct_new       -> connect directly to Postgres for every request, then close
direct_pool      -> use an async client-side pool directly against Postgres
pgbouncer_new    -> connect to PgBouncer for every request, then close
pgbouncer_pool   -> use an async client-side pool in front of PgBouncer
```

The benchmark issues a tiny indexed lookup:

```sql
SELECT payload FROM benchmark_items WHERE id = $1;
```

This intentionally makes connection overhead visible. If the query itself were expensive, query execution would hide most pooling effects.

## Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
docker compose up -d
python scripts/benchmark.py --setup --qps 100 500 1000 2500 --duration 3
```

The compose file exposes:

```text
Postgres  -> 127.0.0.1:25432
PgBouncer -> 127.0.0.1:26432
```

Try a 100k-QPS open-loop simulation:

```bash
python scripts/benchmark.py \
  --qps 10000 25000 50000 100000 \
  --duration 2 \
  --pool-size 128 \
  --max-in-flight 20000 \
  --modes direct_pool pgbouncer_pool
```

The benchmark reports target QPS, scheduled requests, completed requests, errors, achieved QPS, and p50/p95/p99/max latency. Local hardware may not actually sustain 100k successful PostgreSQL requests per second; the point is to show where each mode saturates.

## Methodology

Detailed methodology is documented in [docs/methodology.md](docs/methodology.md). Checked local results are in [docs/benchmark-results.md](docs/benchmark-results.md).

This is an open-loop load generator:

1. Schedule requests at the target QPS.
2. Bound in-flight requests with `--max-in-flight`.
3. Measure per-request latency around the database operation.
4. Report achieved QPS and errors separately from requested QPS.

Use multiple QPS points. Connection pooling often looks unnecessary at low QPS, then becomes decisive when connection churn or backend process pressure dominates.
