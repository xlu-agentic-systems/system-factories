# Benchmark Results

These runs were executed locally against the project PostGIS and Redis containers on May 15, 2026.

The current benchmark seeds a US-wide synthetic dataset into three stores:

- PostgreSQL raw `lat` / `lon` columns with B-tree indexes.
- PostgreSQL PostGIS `geography(Point, 4326)` with a GiST index.
- Redis GEO set queried with `GEOSEARCH`.

## 1M Rows, US-Wide Distribution

Command:

```bash
python3 scripts/run_benchmark.py --rows 1000000 --queries 90 --distribution us-wide --output results/1m-us-redis.json
```

Result:

```text
distribution: us-wide

strategy           queries  avg_ms  p50_ms  p95_ms  max_ms   avg_rows
-----------------  -------  ------  ------  ------  -------  --------
postgis-geography       90  103.833  24.734 365.867 1056.241      43.4
raw-lat-lon             90   57.183  32.610 156.783  461.830      43.4
redis-geosearch         90   18.602   4.423  79.427  236.700      43.4
```

By radius:

```text
strategy           radius_m  queries  avg_ms  p50_ms  p95_ms  max_ms   avg_rows
-----------------  --------  -------  ------  ------  ------  -------  --------
postgis-geography      5000       30   4.914   3.441  14.493   24.473      30.1
postgis-geography     25000       24  37.918  23.397 101.701  239.100      50.0
postgis-geography    100000       36 230.209 197.781 505.842 1056.241      50.0
raw-lat-lon            5000       30  13.917  13.619  30.395   30.994      30.1
raw-lat-lon           25000       24  36.369  30.756  68.933   84.846      50.0
raw-lat-lon          100000       36 107.115  86.760 219.225  461.830      50.0
redis-geosearch        5000       30   2.064   1.830   4.389    4.828      30.1
redis-geosearch       25000       24   7.051   4.278  18.762   52.073      50.0
redis-geosearch      100000       36  40.083  25.299 106.266  236.700      50.0
```

Sample PostgreSQL plan summary:

```text
raw-lat-lon:
  execution_time_ms: 76.358
  root_node: Limit
  index_nodes:
    - Bitmap Heap Scan
    - Bitmap Index Scan: raw_locations_lat_lon_idx

postgis-geography:
  execution_time_ms: 280.630
  root_node: Limit
  index_nodes:
    - Bitmap Heap Scan
    - Bitmap Index Scan: postgis_locations_geom_gist_idx
```

## Interpretation

Redis GEO is fastest in this benchmark because it is an in-memory serving index built for nearby lookup. That speed comes with a system tradeoff: Redis is not the durable source of truth here, so the application must keep it synchronized with the database.

Raw `lat/lon` is still faster than PostGIS on aggregate for this specific workload. The raw query is specialized: it uses a bounding-box prefilter and direct spherical haversine math. For simple radius search, that can be very competitive.

PostGIS wins on expressiveness and correctness rather than raw speed in this benchmark. It gives the database first-class geospatial types, spatial indexes, coordinate-system support, and a path to richer queries such as polygon containment and geofencing.

Practical takeaway:

- Use raw `lat/lon` when the query is simple and you want minimal infrastructure.
- Use PostGIS when location is durable product behavior and geospatial correctness/composability matter.
- Use Redis GEO when you need a very fast nearby-serving index and can handle synchronization from the source of truth.

