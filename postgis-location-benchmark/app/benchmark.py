from __future__ import annotations

import argparse
import json
import statistics
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from app.geo import Location, QueryPoint, bounding_box, generate_locations, generate_query_points, percentile
from app.sql import INDEX_SQL, POSTGIS_RADIUS_SQL, RAW_RADIUS_SQL, SCHEMA_SQL

if TYPE_CHECKING:
    import psycopg


@dataclass(frozen=True)
class QueryMetric:
    strategy: str
    radius_m: int
    rows: int
    elapsed_ms: float


@dataclass(frozen=True)
class StrategySummary:
    strategy: str
    queries: int
    avg_ms: float
    p50_ms: float
    p95_ms: float
    max_ms: float
    avg_rows: float


def run_benchmark(
    dsn: str,
    redis_url: str,
    rows: int,
    queries: int,
    seed: int,
    distribution: str,
    reset: bool,
    output: Path | None,
) -> dict[str, Any]:
    import psycopg
    import redis

    query_points = generate_query_points(queries, seed=seed + 1, distribution=distribution)
    redis_client = redis.Redis.from_url(redis_url, decode_responses=True)
    with psycopg.connect(dsn, autocommit=True) as conn:
        if reset:
            locations = generate_locations(rows, seed=seed, distribution=distribution)
            seed_database(conn, locations)
            seed_redis(redis_client, locations)
        metrics = benchmark_queries(conn, redis_client, query_points)
        plans = explain_sample_queries(conn, query_points[0])

    summaries = summarize(metrics)
    result = {
        "rows_seeded": rows if reset else None,
        "query_count": queries,
        "distribution": distribution,
        "summaries": [asdict(summary) for summary in summaries],
        "by_radius": summarize_by_radius(metrics),
        "plans": plans,
    }
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


def seed_database(conn: "psycopg.Connection", locations: list[Location]) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
        raw_rows = [(loc.id, loc.lat, loc.lon, loc.city) for loc in locations]
        postgis_rows = [(loc.id, loc.lon, loc.lat, loc.city) for loc in locations]
        cur.executemany(
            """
            INSERT INTO raw_locations (id, lat, lon, city)
            VALUES (%s, %s, %s, %s)
            """,
            raw_rows,
        )
        cur.executemany(
            """
            INSERT INTO postgis_locations (id, geom, city)
            VALUES (%s, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, %s)
            """,
            postgis_rows,
        )
        cur.execute(INDEX_SQL)


def seed_redis(redis_client: Any, locations: list[Location], key: str = "locations:geo") -> None:
    redis_client.delete(key)
    pipe = redis_client.pipeline(transaction=False)
    for index, loc in enumerate(locations, start=1):
        pipe.geoadd(key, (loc.lon, loc.lat, str(loc.id)))
        if index % 10_000 == 0:
            pipe.execute()
    pipe.execute()


def benchmark_queries(conn: "psycopg.Connection", redis_client: Any, query_points: list[QueryPoint]) -> list[QueryMetric]:
    metrics: list[QueryMetric] = []
    with conn.cursor() as cur:
        for query in query_points:
            raw_params = _raw_params(query)
            started = time.perf_counter()
            cur.execute(RAW_RADIUS_SQL, raw_params)
            raw_rows = cur.fetchall()
            metrics.append(
                QueryMetric(
                    strategy="raw-lat-lon",
                    radius_m=query.radius_m,
                    rows=len(raw_rows),
                    elapsed_ms=(time.perf_counter() - started) * 1000,
                )
            )

            started = time.perf_counter()
            cur.execute(POSTGIS_RADIUS_SQL, _point_params(query))
            postgis_rows = cur.fetchall()
            metrics.append(
                QueryMetric(
                    strategy="postgis-geography",
                    radius_m=query.radius_m,
                    rows=len(postgis_rows),
                    elapsed_ms=(time.perf_counter() - started) * 1000,
                )
            )

            started = time.perf_counter()
            redis_rows = redis_geosearch(redis_client, query)
            metrics.append(
                QueryMetric(
                    strategy="redis-geosearch",
                    radius_m=query.radius_m,
                    rows=len(redis_rows),
                    elapsed_ms=(time.perf_counter() - started) * 1000,
                )
            )
    return metrics


def redis_geosearch(redis_client: Any, query: QueryPoint, key: str = "locations:geo") -> list[Any]:
    return redis_client.execute_command(
        "GEOSEARCH",
        key,
        "FROMLONLAT",
        query.lon,
        query.lat,
        "BYRADIUS",
        query.radius_m,
        "m",
        "ASC",
        "COUNT",
        50,
        "WITHDIST",
        "WITHCOORD",
    )


def explain_sample_queries(conn: "psycopg.Connection", query: QueryPoint) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + RAW_RADIUS_SQL, _raw_params(query))
        raw_plan = cur.fetchone()[0][0]
        cur.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + POSTGIS_RADIUS_SQL, _point_params(query))
        postgis_plan = cur.fetchone()[0][0]
    return {
        "sample_query": asdict(query),
        "raw_lat_lon": _plan_summary(raw_plan),
        "postgis_geography": _plan_summary(postgis_plan),
    }


def summarize(metrics: list[QueryMetric]) -> list[StrategySummary]:
    summaries: list[StrategySummary] = []
    for strategy in sorted({metric.strategy for metric in metrics}):
        selected = [metric for metric in metrics if metric.strategy == strategy]
        times = [metric.elapsed_ms for metric in selected]
        rows = [metric.rows for metric in selected]
        summaries.append(
            StrategySummary(
                strategy=strategy,
                queries=len(selected),
                avg_ms=round(statistics.mean(times), 3),
                p50_ms=round(percentile(times, 0.50), 3),
                p95_ms=round(percentile(times, 0.95), 3),
                max_ms=round(max(times), 3),
                avg_rows=round(statistics.mean(rows), 1),
            )
        )
    return summaries


def summarize_by_radius(metrics: list[QueryMetric]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    keys = sorted({(metric.strategy, metric.radius_m) for metric in metrics})
    for strategy, radius_m in keys:
        selected = [metric for metric in metrics if metric.strategy == strategy and metric.radius_m == radius_m]
        times = [metric.elapsed_ms for metric in selected]
        counts = [metric.rows for metric in selected]
        rows.append(
            {
                "strategy": strategy,
                "radius_m": radius_m,
                "queries": len(selected),
                "avg_ms": round(statistics.mean(times), 3),
                "p50_ms": round(percentile(times, 0.50), 3),
                "p95_ms": round(percentile(times, 0.95), 3),
                "max_ms": round(max(times), 3),
                "avg_rows": round(statistics.mean(counts), 1),
            }
        )
    return rows


def format_result(result: dict[str, Any]) -> str:
    lines = [
        f"distribution: {result['distribution']}",
        "",
        "strategy           queries  avg_ms  p50_ms  p95_ms  max_ms  avg_rows",
        "-----------------  -------  ------  ------  ------  ------  --------",
    ]
    for summary in result["summaries"]:
        lines.append(
            f"{summary['strategy']:<17}  "
            f"{summary['queries']:>7}  "
            f"{summary['avg_ms']:>6.3f}  "
            f"{summary['p50_ms']:>6.3f}  "
            f"{summary['p95_ms']:>6.3f}  "
            f"{summary['max_ms']:>6.3f}  "
            f"{summary['avg_rows']:>8.1f}"
        )
    lines.extend(
        [
            "",
            "By radius",
            "strategy           radius_m  queries  avg_ms  p50_ms  p95_ms  max_ms  avg_rows",
            "-----------------  --------  -------  ------  ------  ------  ------  --------",
        ]
    )
    for row in result["by_radius"]:
        lines.append(
            f"{row['strategy']:<17}  "
            f"{row['radius_m']:>8}  "
            f"{row['queries']:>7}  "
            f"{row['avg_ms']:>6.3f}  "
            f"{row['p50_ms']:>6.3f}  "
            f"{row['p95_ms']:>6.3f}  "
            f"{row['max_ms']:>6.3f}  "
            f"{row['avg_rows']:>8.1f}"
        )
    lines.extend(
        [
            "",
            "Sample plan",
            f"raw-lat-lon:       {result['plans']['raw_lat_lon']}",
            f"postgis-geography: {result['plans']['postgis_geography']}",
        ]
    )
    return "\n".join(lines)


def _raw_params(query: QueryPoint) -> dict[str, float]:
    min_lat, max_lat, min_lon, max_lon = bounding_box(query.lat, query.lon, query.radius_m)
    return {
        "lat": query.lat,
        "lon": query.lon,
        "radius_m": query.radius_m,
        "min_lat": min_lat,
        "max_lat": max_lat,
        "min_lon": min_lon,
        "max_lon": max_lon,
    }


def _point_params(query: QueryPoint) -> dict[str, float]:
    return {"lat": query.lat, "lon": query.lon, "radius_m": query.radius_m}


def _plan_summary(plan: dict[str, Any]) -> dict[str, Any]:
    root = plan["Plan"]
    return {
        "execution_time_ms": round(float(plan["Execution Time"]), 3),
        "root_node": root["Node Type"],
        "total_cost": root["Total Cost"],
        "plan_rows": root["Plan Rows"],
        "actual_rows": root["Actual Rows"],
        "index_nodes": _collect_index_nodes(root),
    }


def _collect_index_nodes(node: dict[str, Any]) -> list[str]:
    found: list[str] = []
    node_type = node.get("Node Type", "")
    if "Index" in node_type or "Bitmap" in node_type:
        index_name = node.get("Index Name")
        found.append(f"{node_type}: {index_name}" if index_name else node_type)
    for child in node.get("Plans", []):
        found.extend(_collect_index_nodes(child))
    return found


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark raw lat/lon SQL, PostGIS geography, and Redis GEOSEARCH.")
    parser.add_argument("--dsn", default="postgresql://postgres:postgres@127.0.0.1:55432/locations")
    parser.add_argument("--redis-url", default="redis://127.0.0.1:56379/0")
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--queries", type=int, default=60)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--distribution", choices=["us-wide", "city-clustered"], default="us-wide")
    parser.add_argument("--no-reset", action="store_true", help="reuse existing tables instead of reseeding")
    parser.add_argument("--output", default="results/latest.json")
    args = parser.parse_args()

    result = run_benchmark(
        dsn=args.dsn,
        redis_url=args.redis_url,
        rows=args.rows,
        queries=args.queries,
        seed=args.seed,
        distribution=args.distribution,
        reset=not args.no_reset,
        output=Path(args.output) if args.output else None,
    )
    print(format_result(result))


if __name__ == "__main__":
    main()
