from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from app.collision import CollisionStats, expected_collision_pairs, simulate_in_memory
from app.encoder import BASE36_ALPHABET, BASE62_ALPHABET
from app.generator import generate_long_urls
from app.postgres_store import BatchInsertResult, PostgresUrlStore


CODE_SPACES = {
    "base62": len(BASE62_ALPHABET) ** 8,
    "base36": len(BASE36_ALPHABET) ** 8,
}


@dataclass
class AggregateResult:
    method: str
    input_urls: int = 0
    inserted: int = 0
    collisions: int = 0
    retry_successes: int = 0
    failures: int = 0
    elapsed_seconds: float = 0.0

    def add(self, result: BatchInsertResult) -> None:
        self.input_urls += result.input_urls
        self.inserted += result.inserted
        self.collisions += result.collisions
        self.retry_successes += result.retry_successes
        self.failures += result.failures
        self.elapsed_seconds += result.elapsed_seconds


def print_expected(total: int, methods: list[str]) -> None:
    print("expected_collision_pairs")
    for method in methods:
        space = CODE_SPACES[method]
        expected = expected_collision_pairs(total, space)
        print(f"{method}: code_space={space:,} expected_pairs={expected:,.2f}")


def run_memory(total: int, methods: list[str], max_retries: int) -> None:
    print_expected(total, methods)
    for method in methods:
        stats: CollisionStats = simulate_in_memory(
            generate_long_urls(total),
            method=method,
            max_retries=max_retries,
        )
        print_result(
            method=stats.method,
            input_urls=stats.total_urls,
            inserted=stats.inserted,
            collisions=stats.collisions,
            retry_successes=stats.retry_successes,
            failures=stats.failures,
            elapsed_seconds=stats.elapsed_seconds,
        )


def run_postgres(args) -> None:
    store = PostgresUrlStore(args.dsn)
    store.init_schema(reset=args.reset)
    print_expected(args.total, args.methods)

    for method in args.methods:
        aggregate = AggregateResult(method=method)
        processed = 0
        while processed < args.total:
            chunk = min(args.chunk_size, args.total - processed)
            result = store.insert_urls_with_retries(
                generate_long_urls(chunk, start=processed),
                method=method,
                batch_size=args.batch_size,
                max_retries=args.max_retries,
            )
            aggregate.add(result)
            processed += chunk
            print(
                f"progress method={method} processed={processed:,}/{args.total:,} "
                f"inserted={aggregate.inserted:,} collisions={aggregate.collisions:,} "
                f"failures={aggregate.failures:,}",
                flush=True,
            )

        print_result(
            method=aggregate.method,
            input_urls=aggregate.input_urls,
            inserted=aggregate.inserted,
            collisions=aggregate.collisions,
            retry_successes=aggregate.retry_successes,
            failures=aggregate.failures,
            elapsed_seconds=aggregate.elapsed_seconds,
        )


def print_result(
    method: str,
    input_urls: int,
    inserted: int,
    collisions: int,
    retry_successes: int,
    failures: int,
    elapsed_seconds: float,
) -> None:
    rate = input_urls / elapsed_seconds if elapsed_seconds > 0 else 0
    print(f"\nresult method={method}")
    print(f"input_urls={input_urls:,}")
    print(f"inserted={inserted:,}")
    print(f"collisions={collisions:,}")
    print(f"retry_successes={retry_successes:,}")
    print(f"failures_after_retries={failures:,}")
    print(f"elapsed_seconds={elapsed_seconds:,.2f}")
    print(f"throughput_urls_per_sec={rate:,.0f}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark URL shortener hash collision behavior.")
    parser.add_argument("--backend", choices=("postgres", "memory", "math"), default="postgres")
    parser.add_argument("--total", type=int, default=100_000_000)
    parser.add_argument("--methods", nargs="+", choices=("base62", "base36"), default=["base62", "base36"])
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--batch-size", type=int, default=50_000)
    parser.add_argument("--chunk-size", type=int, default=1_000_000)
    parser.add_argument("--reset", action="store_true")
    parser.add_argument(
        "--dsn",
        default=os.getenv(
            "URL_SHORTENER_DATABASE_URL",
            "postgresql://url_shortener:url_shortener@127.0.0.1:15432/url_shortener",
        ),
    )
    args = parser.parse_args()

    if args.backend == "math":
        print_expected(args.total, args.methods)
        return
    if args.backend == "memory":
        run_memory(args.total, args.methods, args.max_retries)
        return
    run_postgres(args)


if __name__ == "__main__":
    main()
