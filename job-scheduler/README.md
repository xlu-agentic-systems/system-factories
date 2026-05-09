# Job Scheduler MVP

This is a Python implementation of the high-level job scheduler design. It intentionally stops before the deep-dive scaling work: there is no write sharding, no distributed lock service, no SQS/Kafka layer, and no cancellation/rescheduling API.

## What It Implements

- Users can create jobs that run immediately, at a specific datetime, or on a cron schedule.
- DynamoDB stores durable job definitions and execution instances.
- Redis sorted sets act as the near-term delay queue, scored by due time.
- A scheduler process looks ahead for pending executions and puts them into Redis.
- A worker process claims due executions, runs the registered task, updates status, retries visible failures, and creates the next execution for recurring cron jobs.
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
  "time_bucket": "1715547600",
  "execution_time_key": "1715548800#execution_uuid",
  "execution_id": "execution_uuid",
  "job_id": "job_uuid",
  "user_id": "user_123",
  "scheduled_at": 1715548800,
  "status": "PENDING",
  "attempt": 0
}
```

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
