# SQLite Concurrency State Machine Demo

This project demonstrates backend concurrency control for a hot state transition:

```text
AVAILABLE -> OFFERED -> BUSY -> AVAILABLE
```

The demo uses a local SQLite database as the shared consistency point. Every worker has its own database connection, which is the same shape as multiple service instances racing to claim the same logical resource.

## What It Demonstrates

- A broken check-then-act implementation can produce multiple apparent winners.
- A transaction using `BEGIN IMMEDIATE` is correct, because the check and update happen while holding SQLite's write lock.
- A single atomic conditional update is also correct, because ownership is decided by `UPDATE ... WHERE status = 'AVAILABLE' AND version = ?`.
- The affected row count is the ownership signal: one row means this worker won; zero rows means another worker got there first.

## Project Layout

```text
app/db.py              SQLite schema and connection helpers
app/claims.py          unsafe, transactional, and atomic claim implementations
app/experiment.py      thread/process race runner
scripts/run_experiment.py
tests/test_concurrency.py
```

For a focused explanation of the concurrency-control patterns, see [docs/concurrency-control.md](docs/concurrency-control.md).

## Run It

```bash
cd sqlite-concurrency-state-machine
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest -q
```

Run the experiment:

```bash
python3 scripts/run_experiment.py --workers 32 --mode both --strategy all
```

Example output shape:

```text
strategy      mode        workers  successes  failures  final_status  version  elapsed_ms
------------  ----------  -------  ---------  --------  ------------  -------  ----------
unsafe        threads          32         32         0  OFFERED            32        250.3
transaction   threads          32          1        31  OFFERED             1        876.4
atomic        threads          32          1        31  OFFERED             1        236.1
```

The exact timings vary by machine. The important columns are `successes` and `version`.

## The Three Claim Strategies

### Unsafe Check Then Update

```sql
SELECT status, version FROM drivers WHERE driver_id = ?;
-- app checks status == AVAILABLE
UPDATE drivers SET status = 'OFFERED', version = version + 1 WHERE driver_id = ?;
```

This is intentionally broken. Multiple workers can read `AVAILABLE` before any update commits. SQLite serializes the individual writes, but the business decision was made from stale reads.

### Transactional Claim

```sql
BEGIN IMMEDIATE;
SELECT status, version FROM drivers WHERE driver_id = ?;
UPDATE drivers SET status = 'OFFERED', version = version + 1 WHERE driver_id = ?;
COMMIT;
```

This is correct because `BEGIN IMMEDIATE` acquires the write lock before the read. Other writers wait, then observe the updated state and lose. The tradeoff is that any slow work inside the transaction holds the lock longer.

### Atomic Conditional Update

```sql
UPDATE drivers
SET status = 'OFFERED',
    current_offer_id = ?,
    version = version + 1
WHERE driver_id = ?
  AND status = 'AVAILABLE'
  AND version = ?;
```

This is the optimistic locking pattern. The app may read a candidate version first, but correctness comes from the conditional update. Under concurrency, only one worker affects one row.
