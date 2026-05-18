# System Factories

Reference implementations for system design exercises.

## Projects

- `job-scheduler`: durable job scheduling service with Redis queues and DynamoDB-style storage patterns.
- `projectL_ads_click_aggregation`: ads click redirect and minute-level analytics aggregation service using a Map/Aggregate/Reduce streaming pipeline.
- `youtube-topk-videos`: sharded, batched, precomputed top-k video view aggregation for 1-hour, 1-day, 1-month, and all-time windows.
- `url-shortener-benchmark`: PostgreSQL URL shortener benchmark comparing SHA-256/Base62 and SHA-256/Base36 8-character collision behavior with retry handling.
- `dropbox-like-system`: Dropbox-style file upload/download API with SQLite metadata and a local content-addressed object store.
- `connection-pooling-benchmark`: PostgreSQL and PgBouncer benchmark comparing per-request connection creation with pooled connections across target QPS levels.
- `database-write-path-cpp`: C++ storage-engine simulator for WAL, row locks, and secondary-index maintenance during indexed updates.
- `virtual-waiting-queue`: admin-enabled ticketing waiting room with Redis sorted-set ordering, SSE updates, admission TTLs, and booking-service guard checks.
- `postgres-text-search-benchmark`: PostgreSQL free-text search benchmark comparing ILIKE scans, B-tree prefix behavior, full-text GIN, and pg_trgm GIN indexes.
