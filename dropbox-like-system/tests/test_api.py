from __future__ import annotations

import hashlib

from fastapi.testclient import TestClient

from app import api
from app.config import settings


def make_client(tmp_path, monkeypatch) -> TestClient:
    monkeypatch.setattr(settings, "data_dir", tmp_path)
    monkeypatch.setattr(settings, "db_path", tmp_path / "metadata.sqlite3")
    monkeypatch.setattr(settings, "objects_dir", tmp_path / "objects")
    monkeypatch.setattr(settings, "max_upload_bytes", 1024 * 1024)
    return TestClient(api.app)


def test_upload_returns_metadata_and_location(tmp_path, monkeypatch) -> None:
    client = make_client(tmp_path, monkeypatch)
    payload = b"hello dropbox"

    response = client.post(
        "/file",
        files={"file": ("hello.txt", payload, "text/plain")},
    )

    assert response.status_code == 201
    body = response.json()
    assert body["file_id"]
    assert body["filename"] == "hello.txt"
    assert body["content_type"] == "text/plain"
    assert body["size_bytes"] == len(payload)
    assert body["sha256"] == hashlib.sha256(payload).hexdigest()
    assert response.headers["location"].endswith(f"/file/{body['file_id']}")


def test_download_streams_file_with_integrity_headers(tmp_path, monkeypatch) -> None:
    client = make_client(tmp_path, monkeypatch)
    payload = b"content to download"
    upload = client.post(
        "/file",
        files={"file": ("download.txt", payload, "text/plain")},
    )
    file_id = upload.json()["file_id"]

    response = client.get(f"/file/{file_id}")

    checksum = hashlib.sha256(payload).hexdigest()
    assert response.status_code == 200
    assert response.content == payload
    assert response.headers["content-type"].startswith("text/plain")
    assert response.headers["content-length"] == str(len(payload))
    assert response.headers["etag"] == f'"sha256:{checksum}"'
    assert response.headers["x-content-sha256"] == checksum
    assert "download.txt" in response.headers["content-disposition"]


def test_download_missing_file_returns_404(tmp_path, monkeypatch) -> None:
    client = make_client(tmp_path, monkeypatch)

    response = client.get("/file/missing")

    assert response.status_code == 404
    assert response.json() == {"detail": "file not found"}


def test_upload_enforces_size_limit(tmp_path, monkeypatch) -> None:
    client = make_client(tmp_path, monkeypatch)
    monkeypatch.setattr(settings, "max_upload_bytes", 4)

    response = client.post(
        "/file",
        files={"file": ("too-large.bin", b"12345", "application/octet-stream")},
    )

    assert response.status_code == 413
    assert response.json() == {"detail": "file exceeds configured upload limit"}

