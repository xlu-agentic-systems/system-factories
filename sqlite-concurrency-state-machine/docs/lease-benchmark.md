# Lease Benchmark

This benchmark compares three ways to protect one hot resource under concurrent workers and service crashes.

## Strategies

### App-Side Locking

`app-lock` uses a process-local in-memory lock table. It can protect threads inside one service process, but it is not shared across service instances. In process mode, every process has its own lock table, so every process can believe it owns the same resource.

Crash behavior is weak. If the process keeps running but the worker dies without releasing the lock, the resource is stuck. If the whole process restarts, the lock disappears, but that means ownership state was never durable.

### DB State Lock With App TTL

`db-state-app-ttl` stores the authoritative lock state in SQLite:

```text
resource_id, state, owner_id, token
```

The state machine is:

```text
FREE -> PENDING -> BUSY
FREE <- PENDING   when the app-side timer fires
```

The database does not store `expires_at` for this strategy. The acquisition path runs under `BEGIN IMMEDIATE` and atomically transitions `FREE -> PENDING`. The app then starts a 15 second timer. If the timer fires and the row is still `PENDING` with the same token, the app transitions it back to `FREE`.

This is different from a DB TTL lease. The DB is the consistency point for the state transition, but the TTL lives in app memory. If the worker crashes while the service process stays alive, the timer can release `PENDING`. If the service process crashes before the timer fires, the timer is lost and the DB row can remain stuck in `PENDING` unless another recovery path exists.

### Redis TTL Lease

`redis-ttl` models the standard Redis lock command:

```text
SET lease:driver-1 <random-token> NX PX 15000
```

The pieces mean:

- `SET`: write a key.
- `NX`: only write if the key does not already exist.
- `PX 15000`: attach a 15,000 millisecond expiration.
- random token: a value only the owner knows, used so release deletes only its own lock.

If Redis returns success, the worker owns the lease. If Redis returns failure, another owner has the key. If the owner crashes, Redis automatically deletes the key when the TTL expires.

Release should be compare-and-delete, not a blind `DEL`:

```lua
if redis.call("GET", key) == token then
  return redis.call("DEL", key)
end
return 0
```

This prevents an old owner from deleting a newer owner's lease after the old TTL expired and the key was reacquired.

The local test uses a SQLite-backed stand-in for Redis TTL so the benchmark can run without a Redis server. The semantics are the same for `SET NX PX` and token-checked release.

To use a real Redis server for the `redis-ttl` strategy, install dependencies from `requirements.txt`, start Redis, and pass `--redis-url`, for example:

```bash
python3 scripts/run_lease_benchmark.py --strategy redis-ttl --mode both --ttl 15 --redis-url redis://127.0.0.1:6379/0
```

## Run

```bash
python3 scripts/run_lease_benchmark.py --workers 32 --ttl 15 --mode both --strategy all
```

For a quick local run:

```bash
python3 scripts/run_lease_benchmark.py --workers 12 --ttl 2 --mode both --strategy all
```

## Expected Shape

```text
Contention
strategy          mode        workers  winners  duplicate_winners  elapsed_ms
----------------  ----------  -------  -------  -----------------  ----------
app-lock          threads          12        1  False                   205.0
app-lock          processes        12       12  True                    260.0
db-state-app-ttl  processes        12        1  False                   300.0
redis-ttl         processes        12        1  False                   290.0

Crash Recovery
strategy          first  before_ttl  after_ttl  app_timer  service_crash  restart_loses_state
----------------  -----  ----------  ---------  ---------  -------------  -------------------
app-lock          True   False       False      False      True           True
db-state-app-ttl  True   False       True       True       False          False
redis-ttl         True   False       True       True       True           False
```

The DB state strategy is concurrency-safe because the database serializes `FREE -> PENDING`, but app-owned TTL is not service-crash-safe by itself. Redis TTL is the most robust of these three for crash recovery because expiration is owned by the external store, not by the app process. Production Redis locking still needs careful TTL sizing, renewal, fencing tokens for side effects, and token-checked release.

