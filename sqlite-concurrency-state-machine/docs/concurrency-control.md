# Concurrency Control Notes

This demo is about controlling business state transitions under concurrent requests. The database is the shared consistency point, so correctness must be enforced by the write that claims ownership.

## State Transition

The example resource is a driver:

```text
AVAILABLE -> OFFERED -> BUSY -> AVAILABLE
```

A match worker is only allowed to claim the driver while it is `AVAILABLE`. If multiple workers race for the same driver, exactly one worker should move the row to `OFFERED`.

## Broken Pattern: Check Then Act

```sql
SELECT status, version
FROM drivers
WHERE driver_id = ?;

UPDATE drivers
SET status = 'OFFERED',
    version = version + 1
WHERE driver_id = ?;
```

The first query observes state, but the second query does not prove the state is still valid. Under concurrency, many workers can read `AVAILABLE` before any one of them commits an update. The database still serializes writes, but it serializes writes that were based on stale business decisions.

## Transactional Pattern

```sql
BEGIN IMMEDIATE;

SELECT status, version
FROM drivers
WHERE driver_id = ?;

UPDATE drivers
SET status = 'OFFERED',
    version = version + 1
WHERE driver_id = ?;

COMMIT;
```

In SQLite, `BEGIN IMMEDIATE` acquires the write lock before the read. That makes the read and update one serialized critical section. It is correct, but it can reduce throughput if slow work is performed inside the transaction.

## Atomic Conditional Update

```sql
UPDATE drivers
SET status = 'OFFERED',
    current_offer_id = ?,
    version = version + 1
WHERE driver_id = ?
  AND status = 'AVAILABLE'
  AND version = ?;
```

This is the optimistic locking pattern. A worker can read the row first to choose a candidate, but ownership is decided by the conditional write. If the update affects one row, the worker won. If it affects zero rows, the worker lost the race or the transition is invalid.

## Practical Rule

Do not separate the validity check from the ownership write when multiple workers can act on the same resource. Put the business precondition in the write path, then use the affected row count as the source of truth.

