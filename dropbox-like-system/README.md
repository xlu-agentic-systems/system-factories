# Dropbox-Like System

Milestone 1 implements the basic file API:

- `POST /file` uploads one new file.
- `GET /file/{file_id}` downloads a previously uploaded file.
- A local device agent watches a Dropbox folder and uploads created/modified files.

The implementation uses FastAPI, SQLite metadata, and a local content-addressed object store on disk. The local object store can later be swapped for S3, MinIO, GCS, or another object store because metadata stores only an immutable object key.

## API Interface

The full REST contract is documented in [docs/api.md](docs/api.md). Summary:

### Upload

```http
POST /file
Content-Type: multipart/form-data

file=<binary file part>
```

Response:

```http
201 Created
Location: http://127.0.0.1:8080/file/{file_id}
Content-Type: application/json
```

```json
{
  "file_id": "3a615c59-1c70-4374-bb30-4b66e92fbc86",
  "filename": "notes.txt",
  "content_type": "text/plain",
  "size_bytes": 12,
  "sha256": "a948904f2f0f479b8f8197694b30184b0d2ed1c1cd2a1ec0fb85d299a192a447",
  "created_at": "2026-05-16T18:00:00Z"
}
```

Best-practice notes:

- Use `multipart/form-data` for binary uploads plus filename/content type metadata.
- Return `201 Created` because a new resource was created.
- Return `Location` pointing at the download URL.
- Return an opaque `file_id`; do not expose object-store paths.
- Return checksum and size so clients can validate integrity.
- Return `413` for files larger than the configured upload limit.

### Download

```http
GET /file/{file_id}
```

Success response:

```http
200 OK
Content-Type: <stored media type>
Content-Length: <size>
ETag: "sha256:<checksum>"
Content-Disposition: attachment; filename*=UTF-8''notes.txt
X-Content-SHA256: <checksum>

<file bytes>
```

Error response:

```http
404 Not Found
Content-Type: application/json

{"detail":"file not found"}
```

Best-practice notes:

- Stream file bytes instead of loading the whole file into memory.
- Preserve the stored content type.
- Include `Content-Length`, `ETag`, and checksum headers.
- Use `Content-Disposition` so browsers keep the original filename.

## Local Object Store Capacity

For this milestone, the local object store is a content-addressed directory under `data/objects`. Capacity is bounded by:

- available local disk space,
- filesystem file count/inode limits,
- OS open-file and directory performance,
- the configured upload limit.

There is no S3-style service limit in this implementation. For S3-compatible local testing, MinIO is the right next step; it can run in Docker and exposes the same style of object API a production deployment would use. Its usable capacity is still the disk/volume size you attach to the container.

## Device Agent

Detailed design notes are in [docs/device-agent.md](docs/device-agent.md).

Each device keeps a local Dropbox folder. The local agent daemon watches that folder through OS file events using `watchdog`:

```text
local Dropbox folder
  -> OS create/modify/move event
  -> device agent debounce/stability check
  -> SHA-256 snapshot
  -> POST /file
  -> local sync state update
```

The agent stores per-device sync state in SQLite so unchanged files are not reuploaded. Temporary, hidden, and partial-download files are ignored.

Run the API server:

```bash
uvicorn app.api:app --host 127.0.0.1 --port 8080 --reload
```

Run a device agent:

```bash
python scripts/device_agent.py \
  --folder ~/DropboxLocal \
  --api-base-url http://127.0.0.1:8080 \
  --state-db data/device-agent.sqlite3 \
  --scan-on-start
```

Create or edit a file under `~/DropboxLocal`; the agent detects the OS event and uploads it through the current API.

## Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.api:app --host 127.0.0.1 --port 8080 --reload
```

Upload:

```bash
curl -i -F "file=@README.md;type=text/plain" http://127.0.0.1:8080/file
```

Download:

```bash
curl -L -o downloaded.bin http://127.0.0.1:8080/file/{file_id}
```

Run tests:

```bash
pytest -q
```
