from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import Iterable

EARTH_RADIUS_M = 6_371_000


@dataclass(frozen=True)
class Location:
    id: int
    lat: float
    lon: float
    city: str


@dataclass(frozen=True)
class QueryPoint:
    lat: float
    lon: float
    radius_m: int


CITY_CENTERS: tuple[tuple[str, float, float, float], ...] = (
    ("new_york", 40.7128, -74.0060, 0.35),
    ("san_francisco", 37.7749, -122.4194, 0.25),
    ("seattle", 47.6062, -122.3321, 0.20),
    ("chicago", 41.8781, -87.6298, 0.30),
    ("austin", 30.2672, -97.7431, 0.25),
)

US_BOUNDS = (24.5, 49.5, -124.8, -66.9)

US_METROS: tuple[tuple[str, float, float, float], ...] = (
    ("new_york", 40.7128, -74.0060, 0.45),
    ("los_angeles", 34.0522, -118.2437, 0.50),
    ("chicago", 41.8781, -87.6298, 0.45),
    ("houston", 29.7604, -95.3698, 0.45),
    ("phoenix", 33.4484, -112.0740, 0.45),
    ("philadelphia", 39.9526, -75.1652, 0.40),
    ("san_antonio", 29.4241, -98.4936, 0.35),
    ("san_diego", 32.7157, -117.1611, 0.35),
    ("dallas", 32.7767, -96.7970, 0.45),
    ("san_jose", 37.3382, -121.8863, 0.30),
    ("austin", 30.2672, -97.7431, 0.35),
    ("seattle", 47.6062, -122.3321, 0.30),
    ("denver", 39.7392, -104.9903, 0.35),
    ("miami", 25.7617, -80.1918, 0.30),
    ("atlanta", 33.7490, -84.3880, 0.40),
    ("minneapolis", 44.9778, -93.2650, 0.35),
)


def generate_locations(count: int, seed: int = 42, distribution: str = "us-wide") -> list[Location]:
    rng = random.Random(seed)
    rows: list[Location] = []
    for row_id in range(1, count + 1):
        if distribution == "city-clustered":
            label, center_lat, center_lon, spread = rng.choice(CITY_CENTERS)
            lat = max(-89.9, min(89.9, rng.gauss(center_lat, spread)))
            lon = _normalize_lon(rng.gauss(center_lon, spread))
        elif distribution == "us-wide":
            if rng.random() < 0.25:
                label, center_lat, center_lon, spread = rng.choice(US_METROS)
                lat = _clamp(rng.gauss(center_lat, spread), US_BOUNDS[0], US_BOUNDS[1])
                lon = _clamp(rng.gauss(center_lon, spread), US_BOUNDS[2], US_BOUNDS[3])
            else:
                label = "us_uniform"
                lat = rng.uniform(US_BOUNDS[0], US_BOUNDS[1])
                lon = rng.uniform(US_BOUNDS[2], US_BOUNDS[3])
        else:
            raise ValueError("distribution must be 'us-wide' or 'city-clustered'")
        rows.append(Location(row_id, round(lat, 7), round(lon, 7), label))
    return rows


def generate_query_points(count: int, seed: int = 7, distribution: str = "us-wide") -> list[QueryPoint]:
    rng = random.Random(seed)
    radii = [5_000, 25_000, 100_000] if distribution == "us-wide" else [1_000, 5_000, 25_000]
    queries: list[QueryPoint] = []
    for _ in range(count):
        if distribution == "city-clustered":
            _label, center_lat, center_lon, spread = rng.choice(CITY_CENTERS)
            lat = rng.gauss(center_lat, spread / 2)
            lon = _normalize_lon(rng.gauss(center_lon, spread / 2))
        elif distribution == "us-wide":
            if rng.random() < 0.50:
                _label, center_lat, center_lon, spread = rng.choice(US_METROS)
                lat = _clamp(rng.gauss(center_lat, spread / 2), US_BOUNDS[0], US_BOUNDS[1])
                lon = _clamp(rng.gauss(center_lon, spread / 2), US_BOUNDS[2], US_BOUNDS[3])
            else:
                lat = rng.uniform(US_BOUNDS[0], US_BOUNDS[1])
                lon = rng.uniform(US_BOUNDS[2], US_BOUNDS[3])
        else:
            raise ValueError("distribution must be 'us-wide' or 'city-clustered'")
        queries.append(
            QueryPoint(
                lat=round(lat, 7),
                lon=round(_normalize_lon(lon), 7),
                radius_m=rng.choice(radii),
            )
        )
    return queries


def bounding_box(lat: float, lon: float, radius_m: int) -> tuple[float, float, float, float]:
    lat_delta = math.degrees(radius_m / EARTH_RADIUS_M)
    cos_lat = math.cos(math.radians(lat))
    lon_delta = 180.0 if abs(cos_lat) < 1e-9 else math.degrees(radius_m / (EARTH_RADIUS_M * cos_lat))
    return (
        max(-90.0, lat - lat_delta),
        min(90.0, lat + lat_delta),
        _normalize_lon(lon - lon_delta),
        _normalize_lon(lon + lon_delta),
    )


def percentile(values: Iterable[float], pct: float) -> float:
    sorted_values = sorted(values)
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    rank = (len(sorted_values) - 1) * pct
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return sorted_values[lower]
    weight = rank - lower
    return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight


def _normalize_lon(lon: float) -> float:
    while lon < -180:
        lon += 360
    while lon > 180:
        lon -= 360
    return lon


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))
