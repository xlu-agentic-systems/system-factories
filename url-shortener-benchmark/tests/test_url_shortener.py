from __future__ import annotations

from app.collision import expected_collision_pairs, simulate_in_memory
from app.encoder import BASE36_ALPHABET, BASE62_ALPHABET, EncodeResult, clean_url, encode_base36, encode_base62
from app.generator import generate_long_urls


def test_clean_url_normalizes_and_removes_tracking_params() -> None:
    assert clean_url(" HTTPS://Example.COM:443//a//b?z=2&utm_source=x&a=1#frag ") == (
        "https://example.com/a/b?a=1&z=2"
    )


def test_encoders_emit_eight_character_codes_in_expected_alphabet() -> None:
    url = "https://example.com/a?v=1"

    base62 = encode_base62(url).short_url
    base36 = encode_base36(url).short_url

    assert len(base62) == 8
    assert len(base36) == 8
    assert set(base62) <= set(BASE62_ALPHABET)
    assert set(base36) <= set(BASE36_ALPHABET)


def test_url_generator_produces_distinct_canonical_urls() -> None:
    urls = [clean_url(url) for url in generate_long_urls(100)]

    assert len(set(urls)) == 100


def test_expected_collision_pairs_for_100m_scale() -> None:
    base62_expected = expected_collision_pairs(100_000_000, len(BASE62_ALPHABET) ** 8)
    base36_expected = expected_collision_pairs(100_000_000, len(BASE36_ALPHABET) ** 8)

    assert 20 < base62_expected < 30
    assert 1_700 < base36_expected < 1_800


def test_retry_path_records_collision_success(monkeypatch) -> None:
    def fake_encoder(_method):
        def encode(url: str, attempt: int = 0) -> EncodeResult:
            if attempt == 0:
                return EncodeResult(short_url="aaaaaaaa", canonical_url=url, attempt=attempt)
            return EncodeResult(short_url=f"aaaaaaa{url[-1]}", canonical_url=url, attempt=attempt)

        return encode

    monkeypatch.setattr("app.collision.encoder_for", fake_encoder)

    stats = simulate_in_memory(["url_1", "url_2", "url_3"], method="base62", max_retries=3)

    assert stats.collisions == 2
    assert stats.retry_successes == 2
    assert stats.failures == 0
    assert stats.inserted == 3

