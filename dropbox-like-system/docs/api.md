# REST API Contract

Milestone 1 exposes two file resource operations:

```text
POST /file
GET  /file/{file_id}
```

The routes intentionally follow the requested singular path shape. In a production public API, `POST /files` and `GET /files/{file_id}` would be the more conventional plural resource form. The semantics below still follow normal REST behavior: create a resource with `POST`, return `201 Created`, identify the created resource with `Location`, and retrieve it with `GET`.

## POST /file

Uploads one new file and creates a new file resource.

### Request

```http
POST /file HTTP/1.1
Host: 127.0.0.1:8080
Content-Type: multipart/form-data; boundary=...

--...
Content-Disposition: form-data; name="file"; filename="notes.txt"
Content-Type: text/plain

hello world
--...--
```

The request body must be `multipart/form-data` with exactly one required file part:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `file` | binary file part | yes | File bytes plus client filename/content type metadata. |

### Success Response

```http
HTTP/1.1 201 Created
Location: http://127.0.0.1:8080/file/3a615c59-1c70-4374-bb30-4b66e92fbc86
Content-Type: application/json
```

```json
{
  "file_id": "3a615c59-1c70-4374-bb30-4b66e92fbc86",
  "filename": "notes.txt",
  "content_type": "text/plain",
  "size_bytes": 11,
  "sha256": "b94d27b9934d3e08a52e52d7da7dabfade...",
  "created_at": "2026-05-16T18:00:00Z"
}
```

Response fields:

| Field | Type | Description |
| --- | --- | --- |
| `file_id` | string | Opaque UUID file identifier. Clients should not infer storage paths from it. |
| `filename` | string | Sanitized basename of the uploaded filename. |
| `content_type` | string | Uploaded media type, or `application/octet-stream` when absent. |
| `size_bytes` | integer | Stored byte length. |
| `sha256` | string | SHA-256 checksum of stored bytes. |
| `created_at` | RFC 3339 timestamp | Server-side creation time. |

### Error Responses

```http
HTTP/1.1 413 Request Entity Too Large
Content-Type: application/json

{"detail":"file exceeds configured upload limit"}
```

FastAPI also returns `422 Unprocessable Entity` when the multipart request is malformed or the required `file` part is missing.

### API Design Notes

- `multipart/form-data` is used because the endpoint receives binary content plus filename/content-type metadata.
- `201 Created` is used because upload creates a new server-side resource.
- `Location` points to the canonical download URL for the new resource.
- `file_id` is opaque. Object-store keys and filesystem paths are not exposed.
- The checksum is returned so clients can verify upload integrity.
- Uploads are streamed to a temporary file and hashed incrementally, avoiding loading full files into memory.

## GET /file/{file_id}

Downloads a file by opaque file id.

### Request

```http
GET /file/3a615c59-1c70-4374-bb30-4b66e92fbc86 HTTP/1.1
Host: 127.0.0.1:8080
```

Path parameters:

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `file_id` | string | yes | Opaque file id returned by `POST /file`. |

### Success Response

```http
HTTP/1.1 200 OK
Content-Type: text/plain
Content-Length: 11
ETag: "sha256:b94d27b9934d3e08a52e52d7da7dabfade..."
Content-Disposition: attachment; filename*=UTF-8''notes.txt
X-Content-SHA256: b94d27b9934d3e08a52e52d7da7dabfade...

hello world
```

Headers:

| Header | Description |
| --- | --- |
| `Content-Type` | Stored media type from upload. |
| `Content-Length` | Stored byte length. |
| `ETag` | Strong checksum-style validator based on SHA-256. |
| `Content-Disposition` | Attachment filename using RFC 5987 `filename*` encoding. |
| `X-Content-SHA256` | Raw SHA-256 checksum for client integrity checks. |

### Error Responses

```http
HTTP/1.1 404 Not Found
Content-Type: application/json

{"detail":"file not found"}
```

### API Design Notes

- The endpoint streams bytes from the object store instead of buffering the file.
- `Content-Disposition: attachment` keeps browser behavior predictable and preserves the original filename.
- `ETag` and `X-Content-SHA256` make cache validation and integrity verification explicit.
- The response does not reveal the local object-store path.

