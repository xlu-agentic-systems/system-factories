from __future__ import annotations

import os
import sqlite3
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


@dataclass(frozen=True)
class StoredFile:
    file_id: str
    filename: str
    content_type: str
    size_bytes: int
    sha256: str
    object_key: str
    created_at: datetime


class SQLiteFileMetadataStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode = WAL;

                CREATE TABLE IF NOT EXISTS files (
                    file_id TEXT PRIMARY KEY,
                    filename TEXT NOT NULL,
                    content_type TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL CHECK(size_bytes >= 0),
                    sha256 TEXT NOT NULL,
                    object_key TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_files_sha256 ON files(sha256);
                """
            )

    def put(self, stored_file: StoredFile) -> StoredFile:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO files
                    (file_id, filename, content_type, size_bytes, sha256, object_key, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    stored_file.file_id,
                    stored_file.filename,
                    stored_file.content_type,
                    stored_file.size_bytes,
                    stored_file.sha256,
                    stored_file.object_key,
                    stored_file.created_at.isoformat(),
                ),
            )
        return stored_file

    def get(self, file_id: str) -> StoredFile | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT file_id, filename, content_type, size_bytes, sha256, object_key, created_at
                FROM files
                WHERE file_id = ?
                """,
                (file_id,),
            ).fetchone()
        if row is None:
            return None
        return StoredFile(
            file_id=row["file_id"],
            filename=row["filename"],
            content_type=row["content_type"],
            size_bytes=int(row["size_bytes"]),
            sha256=row["sha256"],
            object_key=row["object_key"],
            created_at=datetime.fromisoformat(row["created_at"]),
        )


class LocalObjectStore:
    """Content-addressed local object store used as a local object-store stand-in."""

    def __init__(self, objects_dir: str | Path) -> None:
        self.objects_dir = Path(objects_dir)
        self.objects_dir.mkdir(parents=True, exist_ok=True)

    def object_path(self, object_key: str) -> Path:
        return self.objects_dir / object_key[:2] / object_key[2:4] / object_key

    def put_file(self, source_path: Path, object_key: str) -> None:
        target = self.object_path(object_key)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            source_path.unlink(missing_ok=True)
            return
        temp_target = target.with_suffix(".tmp")
        os.replace(source_path, temp_target)
        os.replace(temp_target, target)

    def open_reader(self, object_key: str, chunk_size: int = 1024 * 1024) -> Iterator[bytes]:
        path = self.object_path(object_key)
        with path.open("rb") as file:
            while chunk := file.read(chunk_size):
                yield chunk

    def exists(self, object_key: str) -> bool:
        return self.object_path(object_key).exists()


def utc_now() -> datetime:
    return datetime.now(UTC)

