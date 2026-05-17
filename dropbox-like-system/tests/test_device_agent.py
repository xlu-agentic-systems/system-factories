from __future__ import annotations

import hashlib

from app.device_agent import (
    DeviceSyncState,
    LocalDropboxAgent,
    LocalFileSnapshot,
    UploadResult,
    should_sync_path,
    snapshot_file,
)


class FakeUploader:
    def __init__(self) -> None:
        self.uploads: list[LocalFileSnapshot] = []

    def upload(self, snapshot: LocalFileSnapshot) -> UploadResult:
        self.uploads.append(snapshot)
        return UploadResult(
            file_id=f"remote_{len(self.uploads)}",
            sha256=snapshot.sha256,
            size_bytes=snapshot.size_bytes,
        )


def make_agent(tmp_path):
    folder = tmp_path / "Dropbox"
    state = DeviceSyncState(tmp_path / "agent.sqlite3")
    uploader = FakeUploader()
    return LocalDropboxAgent(folder, state, uploader.upload, quiet_seconds=0), uploader, folder


def test_scan_once_uploads_new_files_and_records_state(tmp_path) -> None:
    agent, uploader, folder = make_agent(tmp_path)
    path = folder / "notes.txt"
    path.write_text("hello", encoding="utf-8")

    uploaded = agent.scan_once()

    assert uploaded == 1
    assert len(uploader.uploads) == 1
    snapshot = uploader.uploads[0]
    assert snapshot.relative_path == "notes.txt"
    assert snapshot.sha256 == hashlib.sha256(b"hello").hexdigest()


def test_scan_once_does_not_reupload_unchanged_file(tmp_path) -> None:
    agent, uploader, folder = make_agent(tmp_path)
    path = folder / "notes.txt"
    path.write_text("hello", encoding="utf-8")

    assert agent.scan_once() == 1
    assert agent.scan_once() == 0

    assert len(uploader.uploads) == 1


def test_modified_file_is_uploaded_again(tmp_path) -> None:
    agent, uploader, folder = make_agent(tmp_path)
    path = folder / "notes.txt"
    path.write_text("hello", encoding="utf-8")
    agent.scan_once()

    path.write_text("hello again", encoding="utf-8")

    assert agent.process_path(path) is True
    assert len(uploader.uploads) == 2
    assert uploader.uploads[-1].sha256 == hashlib.sha256(b"hello again").hexdigest()


def test_ignored_temp_and_hidden_files_are_not_uploaded(tmp_path) -> None:
    agent, uploader, folder = make_agent(tmp_path)
    (folder / ".hidden").write_text("hidden", encoding="utf-8")
    (folder / "file.tmp").write_text("tmp", encoding="utf-8")
    (folder / "ok.txt").write_text("ok", encoding="utf-8")

    assert agent.scan_once() == 1
    assert [upload.relative_path for upload in uploader.uploads] == ["ok.txt"]


def test_snapshot_file_uses_relative_posix_path(tmp_path) -> None:
    folder = tmp_path / "Dropbox"
    nested = folder / "a" / "b"
    nested.mkdir(parents=True)
    path = nested / "file.txt"
    path.write_text("content", encoding="utf-8")

    snapshot = snapshot_file(path, folder)

    assert snapshot.relative_path == "a/b/file.txt"
    assert should_sync_path(path) is True

