from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


BASE36_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyz"
BASE62_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
TRACKING_PARAMS = {
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
    "utm_campaign",
    "utm_content",
    "utm_medium",
    "utm_source",
    "utm_term",
}


@dataclass(frozen=True)
class EncodeResult:
    short_url: str
    canonical_url: str
    attempt: int


def clean_url(url: str) -> str:
    raw = re.sub(r"\s+", "", url.strip())
    if not raw:
        raise ValueError("url must not be empty")
    if "://" not in raw:
        raw = f"https://{raw}"

    parts = urlsplit(raw)
    scheme = parts.scheme.lower()
    hostname = (parts.hostname or "").lower()
    if not hostname:
        raise ValueError(f"url has no host: {url!r}")

    port = parts.port
    if port and not ((scheme == "http" and port == 80) or (scheme == "https" and port == 443)):
        netloc = f"{hostname}:{port}"
    else:
        netloc = hostname

    path = re.sub(r"/{2,}", "/", parts.path or "/")
    query_pairs = [
        (key, value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if key.lower() not in TRACKING_PARAMS
    ]
    query = urlencode(sorted(query_pairs), doseq=True)
    return urlunsplit((scheme, netloc, path, query, ""))


def base_encode(number: int, alphabet: str) -> str:
    if number < 0:
        raise ValueError("number must be non-negative")
    base = len(alphabet)
    if number == 0:
        return alphabet[0]

    chars: list[str] = []
    while number:
        number, remainder = divmod(number, base)
        chars.append(alphabet[remainder])
    return "".join(reversed(chars))


def fixed_sha256_base(url: str, alphabet: str, attempt: int = 0) -> str:
    canonical = clean_url(url)
    material = canonical if attempt == 0 else f"{canonical}\0retry={attempt}"
    digest_number = int.from_bytes(hashlib.sha256(material.encode("utf-8")).digest(), "big")
    encoded = base_encode(digest_number, alphabet)
    return encoded[:8].rjust(8, alphabet[0])


def encode_base62(url: str, attempt: int = 0) -> EncodeResult:
    return EncodeResult(
        short_url=fixed_sha256_base(url, BASE62_ALPHABET, attempt),
        canonical_url=clean_url(url),
        attempt=attempt,
    )


def encode_base36(url: str, attempt: int = 0) -> EncodeResult:
    return EncodeResult(
        short_url=fixed_sha256_base(url, BASE36_ALPHABET, attempt),
        canonical_url=clean_url(url),
        attempt=attempt,
    )


def encoder_for(method: str):
    if method == "base62":
        return encode_base62
    if method == "base36":
        return encode_base36
    raise ValueError(f"unsupported method: {method}")

