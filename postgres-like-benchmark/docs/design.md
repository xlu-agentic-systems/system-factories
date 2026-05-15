# Design Notes

This benchmark exists to answer one narrow question:

> If the service performs real database writes, does one hot post behave worse than the same write volume distributed across many posts?

For PostgreSQL, yes. A single hot counter row becomes a row-lock serialization point.

## Schema

```sql
CREATE TABLE post_counters (
    post_id TEXT PRIMARY KEY,
    like_count BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE likes (
    user_id TEXT NOT NULL,
    post_id TEXT NOT NULL REFERENCES post_counters(post_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, post_id)
);
```

The `likes` table gives idempotent user/post like state. The `post_counters` table gives a display counter.

Each logical like is written as one transaction:

```sql
INSERT INTO likes (user_id, post_id)
VALUES ($1, $2)
ON CONFLICT DO NOTHING;

UPDATE post_counters
SET like_count = like_count + 1
WHERE post_id = $1;
```

The counter update only runs when the insert succeeds. This models unique users liking a post and avoids double-counting retries for the same `(user_id, post_id)`.

## Why Hot Rows Hurt

PostgreSQL row updates take a row-level lock. If 10,000 workers all run:

```sql
UPDATE post_counters
SET like_count = like_count + 1
WHERE post_id = 'post-000001';
```

then they contend on one row. If the same 10,000 updates are spread across 1,000 rows, PostgreSQL can process many row locks in parallel.

This benchmark uses real PostgreSQL transactions, so it is a real database-write demonstration of hot row versus distributed row contention.

## What This Does Not Model

This is not a DynamoDB partition-capacity benchmark. DynamoDB Local is useful for API compatibility tests, but it does not reproduce managed DynamoDB's partition splitting, adaptive capacity, throttling, replication path, or service-side hot-partition behavior.

This benchmark is specifically the relational database version of the same idea: a single logical counter row can become the write bottleneck even on one PostgreSQL node.

## Production Direction

For a like system at very high scale:

- Keep exact user/post like state for idempotency.
- Avoid one hot counter row for viral posts.
- Use sharded counters or asynchronous aggregation.
- Cache approximate display counts when exact freshness is not required.
- Reconcile exact counts from the `likes` table or an event stream when strong display accuracy is required.
