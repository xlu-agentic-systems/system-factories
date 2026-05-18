# Methodology

This project benchmarks connection pooling against PostgreSQL.

## Why PostgreSQL

PostgreSQL is a process-per-connection database server. Creating a new connection involves TCP setup, PostgreSQL startup/authentication, and backend allocation. That makes it a good target for measuring connection pooling.

SQLite is not used because it is embedded/serverless; there is no database server connection to pool. Redis is not the primary target because high-QPS Redis improvements are often dominated by pipelining/multiplexing rather than classic database connection pooling.

## Stack

Docker Compose starts:

```text
PostgreSQL 16 -> 127.0.0.1:25432
PgBouncer    -> 127.0.0.1:26432
```

PostgreSQL is initialized with md5 host auth so the PgBouncer container can authenticate to it. PgBouncer runs in transaction pooling mode with:

```text
DEFAULT_POOL_SIZE=64
MAX_CLIENT_CONN=10000
POOL_MODE=transaction
```

## Workload

The benchmark seeds:

```sql
benchmark_items(id integer primary key, payload text)
```

Each request runs:

```sql
SELECT payload FROM benchmark_items WHERE id = $1;
```

The query is intentionally tiny. This keeps the benchmark focused on connection setup and pooling behavior rather than query execution cost.

## Modes

```text
direct_new       New client connection directly to PostgreSQL per request.
direct_pool      Async application pool directly to PostgreSQL.
pgbouncer_new    New client connection to PgBouncer per request.
pgbouncer_pool   Async application pool connected to PgBouncer.
```

## Open-Loop Load

The runner schedules requests at a target QPS for a fixed duration:

```text
target_qps -> scheduled requests -> bounded by max_in_flight -> DB request
```

It reports:

- `target_qps`: requested arrival rate.
- `scheduled`: requests submitted by the generator.
- `completed`: successful requests.
- `errors`: failed requests.
- `achieved_qps`: completed / elapsed wall time.
- `p50/p95/p99/max`: database operation latency.

At high target QPS, local hardware may not sustain the requested rate. That is expected. The useful signal is the saturation point: achieved QPS stops rising and tail latency grows sharply.

