# Device Agent

The device agent represents the local Dropbox daemon that runs on each user device.

## Responsibilities

- Maintain a configured local Dropbox folder.
- Subscribe to OS filesystem events for that folder.
- Detect created, modified, and moved files.
- Ignore hidden, temporary, and partial-download files.
- Wait for a file to become stable before upload.
- Hash the file with SHA-256.
- Upload changed files to the backend.
- Store per-device sync state so unchanged files are not uploaded again.

## Flow

```text
User creates/edits local file
  -> OS emits filesystem event
  -> watchdog observer receives event
  -> agent queues path
  -> agent waits until size/mtime stop changing
  -> agent computes SHA-256
  -> agent compares local sync state
  -> agent uploads changed file
  -> agent records remote file id and checksum
```

## Local State

The agent stores state in SQLite:

```text
synced_files(
  relative_path PRIMARY KEY,
  remote_file_id,
  sha256,
  size_bytes,
  mtime_ns,
  uploaded_at
)
```

This state is local to one device. It prevents reuploading the same unchanged file during startup scans or repeated OS modify events.

## Current Upload Path

The current milestone uploads file bytes through:

```text
POST /file multipart/form-data
```

That matches the existing server API, but it is not the final production shape. A larger Dropbox-like system should move to:

```text
create upload session -> presigned object-store PUT URL -> complete upload
```

At that point, the device agent should upload bytes directly to MinIO/S3 and use the backend only for metadata/session coordination.

## Run

```bash
python scripts/device_agent.py \
  --folder ~/DropboxLocal \
  --api-base-url http://127.0.0.1:8080 \
  --state-db data/device-agent.sqlite3 \
  --scan-on-start
```

The agent runs until interrupted.

