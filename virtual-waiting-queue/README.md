# Virtual Waiting Queue

Admin-enabled waiting room in front of a ticket booking page. For high-demand events, users join a Redis sorted-set queue before they can reach seat map selection. Admins admit users in controlled batches, and the Booking Service guard rejects reservation attempts from sessions that have not been admitted.

## What It Implements

- Per-event admin settings to enable or disable the queue.
- Redis sorted sets for FIFO queue ordering by join timestamp.
- SSE stream for server-to-client position updates and admission notifications.
- Batch admission endpoint that removes users from the queue and writes expiring admission markers.
- Reservation endpoint that represents the Booking Service authorization check.
- In-memory backend for unit tests and local service-layer load testing.

## Local Run

Start Redis:

```bash
docker compose up -d
```

Install dependencies:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Use Python 3.11, 3.12, or 3.13 with the pinned FastAPI/Pydantic versions.

Run the API:

```bash
uvicorn app.api:app --host 127.0.0.1 --port 8080 --reload
```

Enable the queue for an event:

```bash
curl -X PUT http://127.0.0.1:8080/admin/events/event_123/queue/settings \
  -H 'Content-Type: application/json' \
  -d '{"enabled": true, "admission_ttl_seconds": 600, "default_admit_limit": 100}'
```

Join with SSE updates:

```bash
curl -N 'http://127.0.0.1:8080/events/event_123/queue/stream?session_id=session_abc'
```

Admit the next batch:

```bash
curl -X POST 'http://127.0.0.1:8080/admin/events/event_123/queue/admit?limit=100'
```

Attempt a reservation:

```bash
curl -X POST http://127.0.0.1:8080/events/event_123/reservations \
  -H 'Content-Type: application/json' \
  -d '{"session_id": "session_abc", "seats": ["A-1", "A-2"]}'
```

Run tests and load test:

```bash
pytest -q
python scripts/load_test.py --users 50000 --join-concurrency 128 --admit-batch-size 1000
```

## Notes

Production deployments should put authentication on admin endpoints, bind admission to a signed session or token, and size admission batches from downstream Booking Service saturation metrics. This prototype keeps those concerns explicit but outside the local exercise.
