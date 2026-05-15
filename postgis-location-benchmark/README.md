# PostGIS Location Benchmark

This experiment compares three ways to answer location-radius queries:

1. Raw `lat` / `lon` columns in a normal relational table.
2. PostGIS `geography(Point, 4326)` with a GiST index.
3. Redis geohash-backed GEO index queried with `GEOSEARCH`.

All three stores are seeded with the same deterministic synthetic locations. By default, the seed is spread broadly across the United States with some metro concentration. The benchmark runs identical radius queries around generated query points and reports latency plus a compact PostgreSQL `EXPLAIN ANALYZE` summary.

See [docs/analysis.md](docs/analysis.md) for interpretation guidance and [docs/benchmark-results.md](docs/benchmark-results.md) for recorded local benchmark results.

## Schema

Raw relational table:

```sql
CREATE TABLE raw_locations (
    id BIGINT PRIMARY KEY,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    city TEXT NOT NULL
);

CREATE INDEX raw_locations_lat_lon_idx ON raw_locations (lat, lon);
CREATE INDEX raw_locations_lon_lat_idx ON raw_locations (lon, lat);
```

PostGIS table:

```sql
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE postgis_locations (
    id BIGINT PRIMARY KEY,
    geom geography(Point, 4326) NOT NULL,
    city TEXT NOT NULL
);

CREATE INDEX postgis_locations_geom_gist_idx
ON postgis_locations
USING GIST (geom);
```

Redis GEO set:

```text
GEOADD locations:geo <lon> <lat> <id>
GEOSEARCH locations:geo FROMLONLAT <lon> <lat> BYRADIUS <radius> m ASC COUNT 50 WITHDIST WITHCOORD
```

## Run

Start PostgreSQL with PostGIS:

```bash
docker compose up -d
```

Install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run tests:

```bash
pytest -q
```

Seed and benchmark:

```bash
python3 scripts/run_benchmark.py --rows 100000 --queries 60 --distribution us-wide --output results/latest.json
```

Run again without reseeding:

```bash
python3 scripts/run_benchmark.py --no-reset --queries 60
```

## What To Expect

The raw `lat/lon` query uses a bounding-box prefilter plus a SQL haversine distance calculation. This can work for simple radius queries, but the database only understands the ordinary numeric indexes. The app and SQL have to handle spherical distance math, antimeridian behavior, and query-shape tuning.

The PostGIS query uses:

```sql
ST_DWithin(geom, query_point, radius_m)
```

With a GiST index, PostgreSQL can use a spatial access method designed for geospatial filtering. PostGIS also gives you correct distance functions, geometry/geography types, coordinate-system handling, and a richer query vocabulary.

The Redis query uses:

```text
GEOSEARCH locations:geo FROMLONLAT <lon> <lat> BYRADIUS <radius> m ASC COUNT 50
```

Redis GEO is useful when the workload is an in-memory nearby lookup and the location index does not need relational joins or transactional coupling with the rest of the data.

Raw columns can be acceptable when location is a display attribute or when the query is extremely simple. PostGIS is the better default when location is part of durable product behavior: nearby search, geofencing, routing-adjacent filters, map features, or anything that will evolve beyond basic storage. Redis GEO is a strong low-latency serving index when you can keep it synchronized.
