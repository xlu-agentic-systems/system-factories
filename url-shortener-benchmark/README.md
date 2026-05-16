# URL Shortener Benchmark

Benchmark two URL short-code generation approaches:

1. Clean the long URL, SHA-256 hash it, Base62 encode the hash, and take the first 8 characters.
2. Clean the long URL, SHA-256 hash it, Base36 encode the hash, and take the first 8 characters.

Both approaches write into PostgreSQL tables with the requested shape:

```sql
CREATE TABLE url_mappings_base62 (
    short_url VARCHAR(8) PRIMARY KEY,
    long_url TEXT NOT NULL UNIQUE
);

CREATE TABLE url_mappings_base36 (
    short_url VARCHAR(8) PRIMARY KEY,
    long_url TEXT NOT NULL UNIQUE
);
```

If a generated `short_url` conflicts with an existing row for a different `long_url`, the benchmark retries with a salted hash input. `max_retries` defaults to `3`. If all retries collide, the long URL is recorded in `collision_failures`.

## Collision Scale

For 100M distinct long URLs:

```text
Base62 8-char code space = 62^8 = 218,340,105,584,896
Expected collision pairs ~= 22.90

Base36 8-char code space = 36^8 = 2,821,109,907,456
Expected collision pairs ~= 1,772.35
```

Base62 has a much larger code space, so it should produce far fewer collisions at the same 8-character length.

## Run PostgreSQL

```bash
docker compose up -d
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

The bundled compose file maps PostgreSQL to `127.0.0.1:15432` to avoid clashing with a local Postgres on the default port.

Run a smoke benchmark:

```bash
python scripts/benchmark.py --backend postgres --reset --total 100000 --chunk-size 50000
```

Run the 100M benchmark:

```bash
python scripts/benchmark.py \
  --backend postgres \
  --reset \
  --total 100000000 \
  --chunk-size 1000000 \
  --batch-size 50000 \
  --max-retries 3
```

This is an intentionally large database run. It will insert into both `url_mappings_base62` and `url_mappings_base36`, so plan for substantial disk, WAL, and runtime.

Print the expected collision math without touching PostgreSQL:

```bash
python scripts/benchmark.py --backend math --total 100000000
```

Run smaller in-memory correctness checks:

```bash
python scripts/benchmark.py --backend memory --total 100000
pytest -q
```

See [docs/experiment-details.md](docs/experiment-details.md) for retry semantics, 100M collision math, and the checked PostgreSQL smoke result.

## Notes

The benchmark uses deterministic generated URLs such as:

```text
HTTPS://Example.COM/articles/000000/000000000001?v=1&utm_source=benchmark#fragment
```

Cleaning lowercases scheme and host, removes fragments and tracking parameters, normalizes repeated slashes in the path, removes default ports, and sorts query parameters. The `v` parameter is preserved so the generated long URLs remain distinct after cleaning.
