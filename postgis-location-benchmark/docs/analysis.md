# Analysis Guide

Use this experiment to compare both performance and implementation complexity across raw SQL, PostGIS, and Redis GEO.

## Raw Latitude/Longitude

The raw table stores two floating point columns and uses regular B-tree indexes. The benchmark query first computes a bounding box:

```sql
WHERE lat BETWEEN min_lat AND max_lat
  AND lon BETWEEN min_lon AND max_lon
```

Then it computes haversine distance in SQL and filters by the requested radius.

Strengths:

- Simple schema.
- No PostgreSQL extension required.
- Good enough for storing and displaying coordinates.

Weaknesses:

- Distance math is hand-written in every query or hidden in custom functions.
- B-tree indexes are not spatial indexes.
- More edge cases: poles, antimeridian crossing, coordinate systems, nearest-neighbor query shape.
- Harder to compose with richer geospatial operations later.

## PostGIS Geography

The PostGIS table stores one `geography(Point, 4326)` column and a GiST index. The benchmark query uses:

```sql
WHERE ST_DWithin(geom, query_point, radius_m)
```

Strengths:

- Database understands the value as a geospatial point.
- GiST index supports spatial filtering.
- Built-in distance and relationship functions.
- Better long-term fit for product features involving location.

Weaknesses:

- Requires the PostGIS extension.
- Slightly more operational surface area.
- Teams need to understand geometry versus geography and coordinate reference systems.

## Redis GEO

Redis stores locations in a sorted set using geohash-like scores and exposes commands such as:

```text
GEOADD locations:geo <lon> <lat> <id>
GEOSEARCH locations:geo FROMLONLAT <lon> <lat> BYRADIUS <radius> m ASC COUNT 50
```

Strengths:

- Very fast in-memory nearby lookup.
- Operationally simple query shape for radius search.
- Useful as a serving index or cache in front of a durable database.

Weaknesses:

- It is not your source of truth unless you design it that way.
- You must keep Redis synchronized with the durable location table.
- Limited query composition compared with SQL/PostGIS.
- No relational joins, transactional coupling, or rich spatial predicates.

## Reading The Benchmark

The benchmark prints average, p50, p95, max latency, and average matched rows for all strategies. It also writes JSON to `results/latest.json`, including compact `EXPLAIN ANALYZE` plan summaries for the PostgreSQL strategies.

Look for:

- Whether PostGIS uses `postgis_locations_geom_gist_idx`.
- Whether raw `lat/lon` uses one of the numeric indexes and how many candidates it filters after distance calculation.
- Whether Redis GEO gives lower p50/p95 when the serving index is already in memory.
- How p95 changes as `--rows` grows.
- How each strategy behaves for small radius queries versus larger radius queries.

The usual conclusion is not only "PostGIS is faster." The stronger conclusion is:

> PostGIS makes location a first-class durable database value. Redis GEO can make nearby lookup very fast as a serving index, but it adds synchronization responsibilities.
