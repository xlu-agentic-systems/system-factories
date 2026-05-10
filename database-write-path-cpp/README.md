# Database Write Path in C++

This project is a small C++ storage-engine simulator for understanding what happens when a database updates a row that also participates in a secondary index.

The motivating SQL is:

```sql
UPDATE videos
SET view_count = view_count + 1
WHERE video_id = ?;
```

Assume:

```text
videos(video_id PRIMARY KEY, view_count, title)
INDEX(view_count)
```

The point is not to build a full SQL database. The point is to make the write path visible:

```text
primary-key lookup
row lock
WAL append
heap row update
secondary index delete
secondary index insert
commit
recovery replay
hot-row contention
```

## What It Implements

- A `VideoTable` with rows containing `video_id`, `view_count`, and `title`.
- A primary B-tree-like index from `video_id` to row id.
- A secondary B-tree-like index from `view_count` to row ids.
- A WAL with `BEGIN`, `UPDATE_ROW`, secondary-index maintenance records, and `COMMIT`.
- Recovery that replays committed WAL updates and ignores uncommitted records.
- Row-level locking for concurrent updates.
- Metrics for logical B-tree work, WAL records/bytes, dirty pages, HOT-like updates, non-HOT updates, and lock wait time.
- A CLI demo for a single update trace, hot-row contention, and sharded counters.

The index implementation uses `std::map` internally, but it records logical B-tree cost as `O(log n)` steps. This keeps the code focused on the database write path rather than page splitting details.

## Build and Test

```bash
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
```

Run the trace demo:

```bash
./build/db_write_path_demo trace
```

Example output shape:

```text
UPDATE videos SET view_count = view_count + 1 WHERE video_id = 42;

1. primary-key B-tree lookup: video_id -> row_id
2. row lock acquired for the target tuple
3. WAL records appended before mutating heap/index state
4. heap row updated: view_count old -> new
5. secondary index delete: remove old view_count entry
6. secondary index insert: add new view_count entry
7. COMMIT WAL record appended

view_count: 100 -> 101
```

Run the hot-row demo:

```bash
./build/db_write_path_demo hot-row 8 2000
```

Run the sharded-counter comparison:

```bash
./build/db_write_path_demo sharded 16 8 2000
```

Or run both:

```bash
./scripts/run_hot_row_demo.sh
```

## Internal Cost Model

For the indexed update:

```text
1. Find row by video_id              -> O(log n)
2. Update row value                  -> O(1)
3. Remove old view_count index entry -> O(log n)
4. Insert new view_count index entry -> O(log n)
```

So one update is approximately:

```text
O(log n)
```

For 10,000 updates:

```text
O(10,000 * log n)
```

It is not `O(n)` per update. The table is not reordered and all rows are not scanned.

## Why It Still Hurts in Practice

The expensive part at scale is not just the asymptotic cost. Each indexed update also creates practical write amplification:

```text
WAL / redo log records
new row version or heap mutation
secondary index delete
secondary index insert
dirty pages
row locking
replication traffic in a real database
possible page splits in a real B+ tree
cache pressure
```

This project models those costs as visible metrics.

## HOT-Like Update Contrast

The project also includes `update_title`, which changes an unindexed column. That path is marked as HOT-like:

```text
primary-key lookup
row lock
WAL begin/commit
heap row update
no secondary index delete
no secondary index insert
```

PostgreSQL's real HOT updates have more constraints, but this simplified contrast captures the important lesson: changing an indexed column usually prevents the cheap path because secondary index entries must remain correct.

## Hot Row Lesson

For a viral video:

```text
10,000 views/sec
-> 10,000 updates/sec to the same row
-> one row lock becomes the serialization point
-> WAL and index maintenance happen for every increment
```

That is why production systems commonly aggregate first:

```text
views -> Kafka/Flink/Redis/counter shards -> batched database flush
```

The `sharded` demo approximates this by spreading increments across many logical rows and summing them later.

## Code Map

- `include/db/table.hpp`, `src/table.cpp`: public table API and write path.
- `include/db/btree.hpp`, `src/btree.cpp`: primary and secondary index wrappers.
- `include/db/wal.hpp`, `src/wal.cpp`: WAL record serialization and replay input.
- `include/db/lock_manager.hpp`, `src/lock_manager.cpp`: row-level locks.
- `include/db/metrics.hpp`, `src/metrics.cpp`: instrumentation.
- `tests/test_write_path.cpp`: focused executable tests without external dependencies.
