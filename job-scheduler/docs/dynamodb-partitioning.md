# DynamoDB Partitioning Notes

This note captures the partitioning decisions for the job scheduler's `Jobs` and `Executions` tables.

## Key Distinction

There are two different kinds of sharding involved:

- DynamoDB physical partitions: internal storage partitions managed by DynamoDB. Backend services do not choose these directly.
- Application-level logical shards: partition key values created by our service, such as `1715547600#shard_03`.

DynamoDB hashes the full partition key value and maps it to an internal physical partition. Our service only controls the logical key value it writes.

## Jobs Table

The `Jobs` table is keyed by `job_id`.

```text
PK = job_id
```

Because `job_id` is UUID-like and high-cardinality, writes naturally spread across many DynamoDB key values:

```json
{
  "job_id": "job_uuid",
  "user_id": "user_123",
  "task_id": "send_email",
  "schedule": {"type": "CRON", "expression": "0 10 * * *"}
}
```

The `Jobs` table usually does not need manual suffix sharding.

## Executions Table Without Suffix Sharding

The simple design uses the time bucket as the DynamoDB partition key.

```text
PK = time_bucket
SK = execution_time_key
```

Example item:

```json
{
  "time_bucket": "1715547600",
  "execution_time_key": "1715548800#execution_1",
  "execution_id": "execution_1",
  "job_id": "job_uuid",
  "status": "PENDING"
}
```

Backend query for executions due between `1715548800` and `1715549100`:

```python
response = executions_table.query(
    KeyConditionExpression=
        Key("time_bucket").eq("1715547600")
        & Key("execution_time_key").between(
            "1715548800#",
            "1715549100#~",
        )
)
```

This is one query per bucket. The downside is that all writes for that hour use the same logical partition key:

```text
1715547600
```

At high write volume, that key can become hot.

## Executions Table With Suffix Sharding

The sharded design makes the actual DynamoDB partition key a combination of bucket and suffix.

```text
PK = time_bucket_shard
SK = execution_time_key
```

Example item:

```json
{
  "time_bucket": "1715547600",
  "time_bucket_shard": "1715547600#shard_03",
  "execution_time_key": "1715548800#execution_1",
  "execution_id": "execution_1",
  "job_id": "job_uuid",
  "status": "PENDING"
}
```

The backend computes the suffix before writing:

```python
time_bucket = (scheduled_at // 3600) * 3600
shard_id = hash(execution_id) % execution_shard_count
time_bucket_shard = f"{time_bucket}#shard_{shard_id:02d}"
```

DynamoDB does not append the suffix. The application writes the final partition key value.

## Querying With Suffix Sharding

In the sharded design, we cannot efficiently query only `time_bucket = 1715547600` unless we add a separate index, which would recreate the hot-key problem.

Instead, the backend fans out over the known shard suffixes.

Example with `execution_shard_count = 4`:

```python
all_items = []

for shard_id in range(4):
    partition_key = f"1715547600#shard_{shard_id:02d}"

    response = executions_table.query(
        KeyConditionExpression=
            Key("time_bucket_shard").eq(partition_key)
            & Key("execution_time_key").between(
                "1715548800#",
                "1715549100#~",
            )
    )

    all_items.extend(response["Items"])

all_items.sort(key=lambda item: item["execution_time_key"])
```

The backend sends four targeted queries:

```text
time_bucket_shard = 1715547600#shard_00
time_bucket_shard = 1715547600#shard_01
time_bucket_shard = 1715547600#shard_02
time_bucket_shard = 1715547600#shard_03
```

This is fanout, not a table scan.

## Fixed Shard Count

The simple design uses the same configured shard count for every time bucket:

```text
execution_shard_count = 16
```

That means every bucket can use:

```text
bucket#shard_00
bucket#shard_01
...
bucket#shard_15
```

The shard keys are not pre-created. They exist only when items are written.

Hot buckets may receive writes across many or all suffixes. Cold buckets may receive writes on only a few suffixes, depending on volume and hash assignment. We still use the same configured suffix range because it keeps scheduler reads predictable:

```python
for shard_id in range(execution_shard_count):
    query(f"{time_bucket}#shard_{shard_id:02d}")
```

Adaptive sharding is possible, but then the scheduler needs metadata that says how many shards each bucket used. Fixed sharding is simpler and is the default design here.

## Capacity Intuition

A single DynamoDB physical partition can handle roughly `1,000` WCU for small writes. If all executions for one hour share a single logical partition key, then `10,000` writes/sec can overload that key.

With 16 logical suffix shards:

```text
10,000 writes/sec / 16 shards = 625 writes/sec per logical shard
```

This gives DynamoDB many distinct keys to distribute internally. It does not guarantee perfect physical placement, but it avoids one hot logical key and gives DynamoDB room to spread load.

## Tradeoff

Without suffix sharding:

```text
simple reads
high hot-key risk
```

With suffix sharding:

```text
better write distribution
read fanout across all known suffixes
```

For this job scheduler, read fanout is acceptable because the scheduler always knows the time bucket and configured shard count.
