# Stress Testing Notes

The local stress harness is in `scripts/stress.py`.

It measures:

- Job creation throughput: `Jobs` write, first `Executions` write, Redis enqueue.
- Execution processing throughput: Redis claim/ack and DynamoDB execution status transitions.

It does not prove AWS DynamoDB production throughput. DynamoDB Local is a single local process and does not model physical partition splits, WCU enforcement, adaptive capacity, or multi-node service behavior.

## Command

```bash
python scripts/stress.py \
  --jobs 10000 \
  --create-concurrency 128 \
  --process-concurrency 128 \
  --pop-batch-size 1000
```

## Local Result

On the local Docker setup used during development:

```text
create_done jobs=10000 seconds=29.36 rate=340.57/sec due_queue=10000
process_done jobs=10000 seconds=51.97 rate=192.41/sec remaining_due=0 processing=0
```

This means the current single-process local implementation does not handle `10k jobs/sec`.

## Observed Bottlenecks

- Each job creation currently performs individual writes instead of batched or async writes.
- Each immediate job also performs a Redis sorted-set enqueue.
- Worker processing performs multiple DynamoDB operations per execution.
- The Python implementation uses synchronous boto3 calls.
- DynamoDB Local and Redis run as local containers, not distributed services.

## What Would Need To Change For 10k/sec

The current implementation demonstrates correctness and access patterns, not production throughput.

To approach `10k jobs/sec`, the implementation would need:

- Multiple API/job creation service instances.
- Multiple worker instances.
- Async or heavily parallelized DynamoDB clients.
- Batch write paths where the product/API semantics permit batching.
- Separate queues/workers by task type or priority.
- Real AWS DynamoDB on-demand/provisioned capacity with enough table and GSI capacity.
- A larger `execution_shard_count`, sized from peak writes/sec plus headroom.
- Production load testing against actual AWS infrastructure, not DynamoDB Local.

The sharded key design is still necessary: it gives DynamoDB many logical partition keys to distribute. But key design alone does not make a single local Python process execute 10k jobs/sec.
