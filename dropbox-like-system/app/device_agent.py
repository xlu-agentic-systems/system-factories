from __future__ import annotations

import argparse
import hashlib
import mimetypes
import os
import sqlite3
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from queue import Empty, Queue
from threading import Event

import httpx


IGNORED_SUFFIXES = {".crdownload", ".download", ".part", ".swp", ".tmp"}
IGNORED_NAMES = {".DS_Store", "Thumbs.db"}


@dataclass(frozen=True)
class LocalFileSnapshot:
    path: Path
    relative_path: str
    size_bytes: int
    mtime_ns: int
    sha256: str


@dataclass(frozen=True)
class UploadResult:
    file_id: str
    sha256: str
    size_bytes: int


class DeviceSyncState:
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
                CREATE TABLE IF NOT EXISTS synced_files (
                    relative_path TEXT PRIMARY KEY,
                    remote_file_id TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    mtime_ns INTEGER NOT NULL,
                    uploaded_at TEXT NOT NULL
                );
                """
            )

    def is_synced(self, snapshot: LocalFileSnapshot) -> bool:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT sha256, size_bytes, mtime_ns
                FROM synced_files
                WHERE relative_path = ?
                """,
                (snapshot.relative_path,),
            ).fetchone()
        return (
            row is not None
            and row["sha256"] == snapshot.sha256
            and int(row["size_bytes"]) == snapshot.size_bytes
            and int(row["mtime_ns"]) == snapshot.mtime_ns
        )

    def mark_synced(self, snapshot: LocalFileSnapshot, result: UploadResult) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO synced_files
                    (relative_path, remote_file_id, sha256, size_bytes, mtime_ns, uploaded_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(relative_path)
                DO UPDATE SET
                    remote_file_id = excluded.remote_file_id,
                    sha256 = excluded.sha256,
                    size_bytes = excluded.size_bytes,
                    mtime_ns = excluded.mtime_ns,
                    uploaded_at = excluded.uploaded_at
                """,
                (
                    snapshot.relative_path,
                    result.file_id,
                    result.sha256,
                    result.size_bytes,
                    snapshot.mtime_ns,
                    datetime.now(UTC).isoformat(),
                ),
            )


class DropboxApiUploadClient:
    def __init__(self, base_url: str, timeout_seconds: float = 30.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def upload(self, snapshot: LocalFileSnapshot) -> UploadResult:
        content_type = mimetypes.guess_type(snapshot.path.name)[0] or "application/octet-stream"
        with snapshot.path.open("rb") as file:
            response = httpx.post(
                f"{self.base_url}/file",
                files={"file": (snapshot.path.name, file, content_type)},
                timeout=self.timeout_seconds,
            )
        response.raise_for_status()
        payload = response.json()
        return UploadResult(
            file_id=payload["file_id"],
            sha256=payload["sha256"],
            size_bytes=int(payload["size_bytes"]),
        )


class LocalDropboxAgent:
    def __init__(
        self,
        folder: str | Path,
        state: DeviceSyncState,
        uploader: Callable[[LocalFileSnapshot], UploadResult],
        quiet_seconds: float = 0.25,
    ) -> None:
        self.folder = Path(folder).resolve()
        self.state = state
        self.uploader = uploader
        self.quiet_seconds = quiet_seconds
        self.folder.mkdir(parents=True, exist_ok=True)

    def scan_once(self) -> int:
        uploaded = 0
        for path in self.iter_files():
            if self.process_path(path):
                uploaded += 1
        return uploaded

    def iter_files(self) -> Iterable[Path]:
        for path in self.folder.rglob("*"):
            if path.is_file() and should_sync_path(path):
                yield path

    def process_path(self, path: str | Path) -> bool:
        path = Path(path)
        if not path.exists() or not path.is_file() or not should_sync_path(path):
            return False
        if not is_relative_to(path.resolve(), self.folder):
            return False

        snapshot = wait_for_stable_snapshot(path, self.folder, self.quiet_seconds)
        if self.state.is_synced(snapshot):
            return False

        result = self.uploader(snapshot)
        self.state.mark_synced(snapshot, result)
        return True

    def watch_forever(self, poll_timeout_seconds: float = 1.0) -> None:
        from watchdog.events import FileSystemEventHandler
        from watchdog.observers import Observer

        queue: Queue[Path] = Queue()
        stop_event = Event()

        class Handler(FileSystemEventHandler):
            def on_created(self, event) -> None:
                if not event.is_directory:
                    queue.put(Path(event.src_path))

            def on_modified(self, event) -> None:
                if not event.is_directory:
                    queue.put(Path(event.src_path))

            def on_moved(self, event) -> None:
                if not event.is_directory:
                    queue.put(Path(event.dest_path))

        observer = Observer()
        observer.schedule(Handler(), str(self.folder), recursive=True)
        observer.start()
        try:
            while not stop_event.is_set():
                try:
                    path = queue.get(timeout=poll_timeout_seconds)
                except Empty:
                    continue
                try:
                    self.process_path(path)
                finally:
                    queue.task_done()
        except KeyboardInterrupt:
            stop_event.set()
        finally:
            observer.stop()
            observer.join()


def should_sync_path(path: Path) -> bool:
    if path.name in IGNORED_NAMES:
        return False
    if path.name.startswith("."):
        return False
    if path.suffix.lower() in IGNORED_SUFFIXES:
        return False
    return not any(part.startswith(".") for part in path.parts)


def wait_for_stable_snapshot(path: Path, root: Path, quiet_seconds: float) -> LocalFileSnapshot:
    previous = stat_pair(path)
    while True:
        time.sleep(quiet_seconds)
        current = stat_pair(path)
        if current == previous:
            return snapshot_file(path, root)
        previous = current


def stat_pair(path: Path) -> tuple[int, int]:
    stat = path.stat()
    return stat.st_size, stat.st_mtime_ns


def snapshot_file(path: Path, root: Path) -> LocalFileSnapshot:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        while chunk := file.read(1024 * 1024):
            digest.update(chunk)
    stat = path.stat()
    return LocalFileSnapshot(
        path=path,
        relative_path=path.resolve().relative_to(root.resolve()).as_posix(),
        size_bytes=stat.st_size,
        mtime_ns=stat.st_mtime_ns,
        sha256=digest.hexdigest(),
    )


def is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a local Dropbox folder sync agent.")
    parser.add_argument("--folder", default=os.getenv("DROPBOX_DEVICE_FOLDER", "Dropbox"))
    parser.add_argument("--api-base-url", default=os.getenv("DROPBOX_API_BASE_URL", "http://127.0.0.1:8080"))
    parser.add_argument("--state-db", default=os.getenv("DROPBOX_AGENT_STATE_DB", "data/device-agent.sqlite3"))
    parser.add_argument("--quiet-seconds", type=float, default=0.25)
    parser.add_argument("--scan-on-start", action="store_true")
    args = parser.parse_args()

    client = DropboxApiUploadClient(args.api_base_url)
    agent = LocalDropboxAgent(
        folder=args.folder,
        state=DeviceSyncState(args.state_db),
        uploader=client.upload,
        quiet_seconds=args.quiet_seconds,
    )
    if args.scan_on_start:
        uploaded = agent.scan_once()
        print(f"initial_scan_uploaded={uploaded}")
    print(f"watching_folder={Path(args.folder).resolve()}")
    agent.watch_forever()


if __name__ == "__main__":
    main()

