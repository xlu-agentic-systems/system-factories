from __future__ import annotations

RAW_RADIUS_SQL = """
WITH params AS (
  SELECT
    %(lat)s::double precision AS qlat,
    %(lon)s::double precision AS qlon,
    %(radius_m)s::double precision AS radius_m,
    %(min_lat)s::double precision AS min_lat,
    %(max_lat)s::double precision AS max_lat,
    %(min_lon)s::double precision AS min_lon,
    %(max_lon)s::double precision AS max_lon
),
candidates AS (
  SELECT
    l.id,
    l.lat,
    l.lon,
    2 * 6371000 * asin(sqrt(
      power(sin(radians((l.lat - p.qlat) / 2)), 2) +
      cos(radians(p.qlat)) * cos(radians(l.lat)) *
      power(sin(radians((l.lon - p.qlon) / 2)), 2)
    )) AS distance_m
  FROM raw_locations l
  CROSS JOIN params p
  WHERE l.lat BETWEEN p.min_lat AND p.max_lat
    AND (
      (p.min_lon <= p.max_lon AND l.lon BETWEEN p.min_lon AND p.max_lon)
      OR
      (p.min_lon > p.max_lon AND (l.lon >= p.min_lon OR l.lon <= p.max_lon))
    )
)
SELECT id, lat, lon, distance_m
FROM candidates
WHERE distance_m <= (SELECT radius_m FROM params)
ORDER BY distance_m
LIMIT 50
"""

POSTGIS_RADIUS_SQL = """
WITH params AS (
  SELECT
    ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)::geography AS q,
    %(radius_m)s::double precision AS radius_m
)
SELECT
  l.id,
  ST_Y(l.geom::geometry) AS lat,
  ST_X(l.geom::geometry) AS lon,
  ST_Distance(l.geom, p.q, false) AS distance_m
FROM postgis_locations l
CROSS JOIN params p
WHERE ST_DWithin(l.geom, p.q, p.radius_m, false)
ORDER BY ST_Distance(l.geom, p.q, false)
LIMIT 50
"""

SCHEMA_SQL = """
CREATE EXTENSION IF NOT EXISTS postgis;

DROP TABLE IF EXISTS raw_locations;
DROP TABLE IF EXISTS postgis_locations;

CREATE TABLE raw_locations (
    id BIGINT PRIMARY KEY,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    city TEXT NOT NULL
);

CREATE TABLE postgis_locations (
    id BIGINT PRIMARY KEY,
    geom geography(Point, 4326) NOT NULL,
    city TEXT NOT NULL
);
"""

INDEX_SQL = """
CREATE INDEX raw_locations_lat_lon_idx ON raw_locations (lat, lon);
CREATE INDEX raw_locations_lon_lat_idx ON raw_locations (lon, lat);
CREATE INDEX postgis_locations_geom_gist_idx ON postgis_locations USING GIST (geom);
ANALYZE raw_locations;
ANALYZE postgis_locations;
"""
