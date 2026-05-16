from __future__ import annotations

from collections.abc import Iterator


def generate_long_urls(total: int, start: int = 0) -> Iterator[str]:
    if total < 0:
        raise ValueError("total must be non-negative")
    if start < 0:
        raise ValueError("start must be non-negative")

    for value in range(start, start + total):
        yield (
            f"HTTPS://Example.COM/articles/{value // 10_000:06d}/{value:012d}"
            f"?v={value}&utm_source=benchmark#fragment"
        )


def batched(iterable, batch_size: int):
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

