# Lease Benchmark

This benchmark compares three ways to protect one hot resource under concurrent workers and service crashes.

## Strategies

### App-Side Locking

`app-lock` uses a process-local in-memory lock table. It can protect threads inside one service process, but it is not shared across service instances. In process mode, every process has its own lock table, so every process can believe it owns the same resource.

Crash behavior is also weak. If the process keeps running but the worker dies without releasing the lock, the resource is stuck. If the whole process restarts, the lock disappears, but that means ownership state was never durable.

### DB TTL Lease

`db-ttl` stores a lease row in SQLite:

```text
resource_id, owner_id, token, expires_at
```

The acquisition path runs under `BEGIN IMMEDIATE`, checks whether the existing lease is expired, and replaces it only when it is missing or expired. If an owner crashes, the row remains until `expires_at`, then another worker can acquire it.

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
strategy    mode        workers  winners  duplicate_winners  elapsed_ms
----------  ----------  -------  -------  -----------------  ----------
app-lock    threads          12        1  False                   205.0
app-lock    processes        12       12  True                    260.0
db-ttl      processes        12        1  False                   300.0
redis-ttl   processes        12        1  False                   290.0

Crash Recovery
strategy    first  before_ttl  after_ttl  recovered  restart_loses_state
----------  -----  ----------  ---------  ---------  -------------------
app-lock    True   False       False      False      True
db-ttl      True   False       True       True       False
redis-ttl   True   False       True       True       False
```

The robust choices are the shared TTL strategies. The DB TTL lease is strongly consistent with the database and is easy to reason about. Redis TTL is fast and recovers from crashes automatically, but production systems also need careful TTL sizing, renewal, fencing tokens for side effects, and token-checked release.
