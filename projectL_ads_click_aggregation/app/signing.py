from __future__ import annotations

import hmac
from hashlib import sha256


def _payload(advertiser_id: str, ad_id: str, impression_id: str) -> bytes:
    return f"{advertiser_id}.{ad_id}.{impression_id}".encode("utf-8")


def sign_impression(
    *, secret: str, advertiser_id: str, ad_id: str, impression_id: str
) -> str:
    return hmac.new(
        secret.encode("utf-8"),
        _payload(advertiser_id, ad_id, impression_id),
        sha256,
    ).hexdigest()


def verify_impression_signature(
    *,
    secret: str,
    advertiser_id: str,
    ad_id: str,
    impression_id: str,
    signature: str,
) -> bool:
    expected = sign_impression(
        secret=secret,
        advertiser_id=advertiser_id,
        ad_id=ad_id,
        impression_id=impression_id,
    )
    return hmac.compare_digest(expected, signature)

