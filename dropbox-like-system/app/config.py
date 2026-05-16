from __future__ import annotations

import os
from pathlib import Path


class Settings:
    def __init__(self) -> None:
        self.data_dir = Path(os.getenv("DROPBOX_DATA_DIR", "data"))
        self.db_path = Path(os.getenv("DROPBOX_DB_PATH", self.data_dir / "metadata.sqlite3"))
        self.objects_dir = Path(os.getenv("DROPBOX_OBJECTS_DIR", self.data_dir / "objects"))
        self.max_upload_bytes = int(os.getenv("DROPBOX_MAX_UPLOAD_BYTES", str(256 * 1024 * 1024)))


settings = Settings()

