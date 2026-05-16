from __future__ import annotations

from app.models import TopKEntry
from app.time_windows import bucket_start


class TopKQueryService:
    def __init__(self, storage) -> None:
        self.storage = storage

    def topk_at(self, window: str, occurred_at: int, k: int) -> list[TopKEntry]:
        return self.storage.topk(window, bucket_start(window, occurred_at), k)

