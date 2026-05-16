from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter

from app.encoder import encoder_for


@dataclass(frozen=True)
class CollisionStats:
    method: str
    total_urls: int
    inserted: int
    collisions: int
    retry_successes: int
    failures: int
    elapsed_seconds: float


class MemoryCollisionStore:
    def __init__(self) -> None:
        self.by_short: dict[str, str] = {}

    def insert(self, short_url: str, long_url: str) -> bool:
        existing = self.by_short.get(short_url)
        if existing is None:
            self.by_short[short_url] = long_url
            return True
        return existing == long_url


def simulate_in_memory(
    urls,
    method: str,
    max_retries: int = 3,
) -> CollisionStats:
    encoder = encoder_for(method)
    store = MemoryCollisionStore()
    started = perf_counter()
    total = 0
    collisions = 0
    retry_successes = 0
    failures = 0

    for url in urls:
        total += 1
        for attempt in range(max_retries + 1):
            result = encoder(url, attempt)
            inserted_or_duplicate = store.insert(result.short_url, result.canonical_url)
            if inserted_or_duplicate:
                if attempt:
                    retry_successes += 1
                break
            collisions += 1
        else:
            failures += 1

    return CollisionStats(
        method=method,
        total_urls=total,
        inserted=len(store.by_short),
        collisions=collisions,
        retry_successes=retry_successes,
        failures=failures,
        elapsed_seconds=perf_counter() - started,
    )


def expected_collision_pairs(total_urls: int, code_space_size: int) -> float:
    if total_urls < 0:
        raise ValueError("total_urls must be non-negative")
    if code_space_size <= 0:
        raise ValueError("code_space_size must be positive")
    return total_urls * (total_urls - 1) / (2 * code_space_size)

