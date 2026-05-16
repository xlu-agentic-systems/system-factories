from __future__ import annotations

from urllib.parse import quote

from fastapi import Depends, FastAPI, File, HTTPException, Request, Response, UploadFile, status
from fastapi.responses import StreamingResponse

from app.config import settings
from app.models import ErrorResponse, FileMetadata
from app.service import FileNotFoundError, FileService, FileTooLargeError
from app.storage import LocalObjectStore, SQLiteFileMetadataStore, StoredFile


app = FastAPI(
    title="Dropbox-Like File API",
    description="Milestone 1: upload a file and download it by opaque file id.",
)


def get_service() -> FileService:
    return FileService(
        metadata_store=SQLiteFileMetadataStore(settings.db_path),
        object_store=LocalObjectStore(settings.objects_dir),
        max_upload_bytes=settings.max_upload_bytes,
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post(
    "/file",
    response_model=FileMetadata,
    status_code=status.HTTP_201_CREATED,
    responses={
        413: {"model": ErrorResponse, "description": "File exceeds upload limit."},
    },
)
async def upload_file(
    request: Request,
    response: Response,
    file: UploadFile = File(..., description="Binary file payload."),
    service: FileService = Depends(get_service),
) -> FileMetadata:
    try:
        stored = await service.upload(file)
    except FileTooLargeError as exc:
        raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail=str(exc)) from exc

    location = str(request.url_for("download_file", file_id=stored.file_id))
    response.headers["Location"] = location
    return to_metadata(stored)


@app.get(
    "/file/{file_id}",
    responses={
        200: {"content": {"application/octet-stream": {}}},
        404: {"model": ErrorResponse, "description": "File id was not found."},
    },
)
def download_file(
    file_id: str,
    service: FileService = Depends(get_service),
) -> StreamingResponse:
    try:
        stored, body = service.download(file_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="file not found") from exc

    quoted_filename = quote(stored.filename)
    headers = {
        "Content-Length": str(stored.size_bytes),
        "ETag": f'"sha256:{stored.sha256}"',
        "Content-Disposition": f"attachment; filename*=UTF-8''{quoted_filename}",
        "X-Content-SHA256": stored.sha256,
    }
    return StreamingResponse(body, media_type=stored.content_type, headers=headers)


def to_metadata(stored: StoredFile) -> FileMetadata:
    return FileMetadata(
        file_id=stored.file_id,
        filename=stored.filename,
        content_type=stored.content_type,
        size_bytes=stored.size_bytes,
        sha256=stored.sha256,
        created_at=stored.created_at,
    )
