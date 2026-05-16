from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class FileMetadata(BaseModel):
    file_id: str = Field(description="Opaque stable file identifier.")
    filename: str = Field(description="Original client-provided filename.")
    content_type: str = Field(description="Stored media type.")
    size_bytes: int = Field(ge=0)
    sha256: str = Field(min_length=64, max_length=64)
    created_at: datetime


class ErrorResponse(BaseModel):
    detail: str

