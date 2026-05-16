# Load Testing

The local load test exercises the service layer without an HTTP server. It creates a popular event, places users into the queue, admits them in batches, and verifies that admitted users pass the Booking Service guard.

Default in-memory run:

```bash
python scripts/load_test.py --users 50000 --join-concurrency 128 --admit-batch-size 1000
```

Redis-backed run:

```bash
docker compose up -d
python scripts/load_test.py --backend redis --users 50000 --join-concurrency 128 --admit-batch-size 1000
```

The script reports join throughput, admission throughput, final queue depth, and reservation-check throughput. The Redis run is the closer local proxy for production because queue ordering and admission state use Redis sorted sets and expiring admission keys.
