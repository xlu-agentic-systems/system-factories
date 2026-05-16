from __future__ import annotations

import hashlib
import tempfile
import uuid
from pathlib import Path

from fastapi import UploadFile

from app.storage import LocalObjectStore, SQLiteFileMetadataStore, StoredFile, utc_now


class FileTooLargeError(ValueError):
    pass


class FileNotFoundError(KeyError):
    pass


class FileService:
    def __init__(
        self,
        metadata_store: SQLiteFileMetadataStore,
        object_store: LocalObjectStore,
        max_upload_bytes: int,
    ) -> None:
        self.metadata_store = metadata_store
        self.object_store = object_store
        self.max_upload_bytes = max_upload_bytes

    async def upload(self, upload_file: UploadFile) -> StoredFile:
        filename = Path(upload_file.filename or "upload.bin").name
        content_type = upload_file.content_type or "application/octet-stream"
        hasher = hashlib.sha256()
        size_bytes = 0

        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = Path(temp_file.name)
            while chunk := await upload_file.read(1024 * 1024):
                size_bytes += len(chunk)
                if size_bytes > self.max_upload_bytes:
                    temp_file.close()
                    temp_path.unlink(missing_ok=True)
                    raise FileTooLargeError("file exceeds configured upload limit")
                hasher.update(chunk)
                temp_file.write(chunk)

        sha256 = hasher.hexdigest()
        self.object_store.put_file(temp_path, sha256)
        stored = StoredFile(
            file_id=str(uuid.uuid4()),
            filename=filename,
            content_type=content_type,
            size_bytes=size_bytes,
            sha256=sha256,
            object_key=sha256,
            created_at=utc_now(),
        )
        return self.metadata_store.put(stored)

    def get_metadata(self, file_id: str) -> StoredFile:
        stored = self.metadata_store.get(file_id)
        if stored is None or not self.object_store.exists(stored.object_key):
            raise FileNotFoundError(file_id)
        return stored

    def download(self, file_id: str):
        stored = self.get_metadata(file_id)
        return stored, self.object_store.open_reader(stored.object_key)

