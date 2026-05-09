# Job Scheduler MVP

This is a Python implementation of the job scheduler design. It includes the high-level API/data flow and the first deep-dive pieces for local execution: DynamoDB write sharding, GSIs for scheduler/user access patterns, Redis sorted-set delay queues, retries, and Redis visibility leases. It does not use AWS SQS; Redis provides the local queue semantics.

## What It Implements

- Users can create jobs that run immediately, at a specific datetime, or on a cron schedule.
- DynamoDB stores durable job definitions and execution instances.
- DynamoDB execution writes are spread across `time_bucket#shard_N` partition keys.
- DynamoDB GSIs support user monitoring, user+status monitoring, pending scheduler scans, and execution lookup by ID.
- Redis sorted sets act as the near-term delay queue and processing lease queue, scored by due time or lease expiry.
- A scheduler process looks ahead for pending executions across bucket shards and puts them into Redis.
- A worker process claims due executions, heartbeats Redis visibility leases, runs the registered task, updates status, retries visible failures, recovers expired leases, and creates the next execution for recurring cron jobs.
- Users can query execution status by `user_id`.

## Core Data Model

`Jobs` stores reusable job definitions:

```json
{
  "job_id": "uuid",
  "user_id": "user_123",
  "task_id": "send_email",
  "schedule": {"type": "CRON", "expression": "0 10 * * *"},
  "parameters": {"to": "john@example.com"}
}
```

`Executions` stores concrete run instances:

```json
{
  "time_bucket_shard": "1715547600#shard_03",
  "execution_time_key": "1715548800#execution_uuid",
  "time_bucket": "1715547600",
  "shard_id": 3,
  "status_time_bucket_shard": "PENDING#1715547600#shard_03",
  "user_status": "user_123#PENDING",
  "execution_id": "execution_uuid",
  "job_id": "job_uuid",
  "user_id": "user_123",
  "scheduled_at": 1715548800,
  "status": "PENDING",
  "attempt": 0
}
```

Execution table indexes:

- Primary key: `time_bucket_shard`, `execution_time_key`
- `user_execution_time_index`: `user_id`, `execution_time_key`
- `user_status_execution_time_index`: `user_status`, `execution_time_key`
- `status_time_bucket_shard_index`: `status_time_bucket_shard`, `execution_time_key`
- `execution_id_index`: `execution_id`

The scheduler queries `status_time_bucket_shard_index` for each shard in the lookahead window. This avoids concentrating all pending execution reads/writes into one hourly partition.

## Local Run

Start Redis and DynamoDB Local:

```bash
docker compose up -d
```

Install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run the API:

```bash
uvicorn app.api:app --host 127.0.0.1 --port 8080 --reload
```

Run the scheduler:

```bash
python -m app.scheduler
```

Run the worker:

```bash
python -m app.worker
```

Run verification:

```bash
pytest -q
python scripts/smoke.py
```

## Example Requests

Create an immediate job:

```bash
curl -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "user_id": "user_123",
    "task_id": "print_message",
    "schedule": {"type": "IMMEDIATE"},
    "parameters": {"message": "hello"}
  }'
```

Create a datetime job:

```bash
curl -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "user_id": "user_123",
    "task_id": "send_email",
    "schedule": {"type": "DATE", "expression": "2026-05-09T10:00:00Z"},
    "parameters": {"to": "john@example.com", "subject": "Daily Report"}
  }'
```

Create a recurring cron job:

```bash
curl -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "user_id": "user_123",
    "task_id": "print_message",
    "schedule": {"type": "CRON", "expression": "*/5 * * * *"},
    "parameters": {"message": "runs every five minutes"}
  }'
```

Check execution status:

```bash
curl 'http://localhost:8080/jobs?user_id=user_123&limit=20'
```

## Registered Tasks

- `print_message`: logs the provided `message` parameter.
- `send_email`: stub task that logs the request and returns a successful result.

Add real task handlers in `app/registry.py`.
