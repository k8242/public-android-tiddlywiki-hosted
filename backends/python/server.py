#!/usr/bin/env python3
"""Minimal TiddlyWiki tiddler-syncing backend in Python 3.

Implements TiddlyWiki's tiddlyweb HTTP protocol, content-addressed binary blob
storage (/files/{sha256}), and a batch delta sync endpoint (/api/sync) for the
Android WorkManager periodic sync client.

Storage layout under DATA_DIR (defaults to <repo_root>/data):
  - tiddlywiki.db: SQLite database (WAL mode) with `tiddlers` and `blobs` tables
  - backup/tiddlers/<encoded_title>.json: Human-readable JSON backup per tiddler
  - backup/blobs/<sha256>: Raw binary file backup per content-addressed attachment
"""

from __future__ import annotations

import datetime
import gzip
import hashlib
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import sqlite3
from typing import Any
import urllib.parse

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_DIR = Path(os.environ.get("DATA_DIR", REPO_ROOT / "data"))
SCHEMA_PATH = Path(__file__).resolve().parents[1] / "schema.sql"

DEFAULT_PORT = 8080
DEFAULT_BATCH_LIMIT = 100
DEFAULT_TIDDLER_TYPE = "text/vnd.tiddlywiki"
DEFAULT_BAG = "default"
DEFAULT_RECIPE = "default"
TIDDLYWIKI_VERSION = "5.3.5"
CANONICAL_URI_PREFIX = "files/"
GZIP_MIN_BYTES = 512

CORE_TIDDLER_KEYS = frozenset({
    "title",
    "revision",
    "modified",
    "type",
    "text",
    "bag",
})


class TiddlerStore:
    """Manages dual SQLite + filesystem backup storage for tiddlers and blobs."""

    def __init__(self, data_dir: Path = DEFAULT_DATA_DIR) -> None:
        self.data_dir = Path(data_dir)
        self.db_path = self.data_dir / "tiddlywiki.db"
        self.tiddler_backup_dir = self.data_dir / "backup" / "tiddlers"
        self.blob_backup_dir = self.data_dir / "backup" / "blobs"
        self.init_storage()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        return conn

    def init_storage(self) -> None:
        """Initialize the SQLite schema and backup directory structure."""
        self.tiddler_backup_dir.mkdir(parents=True, exist_ok=True)
        self.blob_backup_dir.mkdir(parents=True, exist_ok=True)
        schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
        with self._connect() as conn:
            conn.executescript(schema_sql)

    def get_status(self) -> dict[str, Any]:
        """Return tiddlyweb server status metadata (aligned with getStatus / GetStatus)."""
        return {
            "username": "user",
            "space": {"recipe": DEFAULT_RECIPE},
            "tiddlywiki_version": TIDDLYWIKI_VERSION,
        }

    def list_skinny_tiddlers(self) -> list[dict[str, Any]]:
        """Return all non-deleted tiddlers without `text` (aligned with listSkinnyTiddlers)."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT title, revision, modified, type, fields_json, blob_hash
                FROM tiddlers
                WHERE deleted = 0
                ORDER BY modified DESC
                """
            ).fetchall()
        return [self._row_to_tiddler(row, include_text=False) for row in rows]

    def get_tiddler(self, title: str) -> dict[str, Any] | None:
        """Fetch a single full tiddler by title (aligned with getTiddler / GetTiddler)."""
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT title, revision, modified, type, fields_json, text, blob_hash
                FROM tiddlers
                WHERE title = ? AND deleted = 0
                """,
                (title,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_tiddler(row, include_text=True)

    def put_tiddler(self, title: str, payload: dict[str, Any]) -> int:
        """Upsert a tiddler in SQLite and write its JSON backup (aligned with putTiddler)."""
        now_tw = self._current_tw_timestamp()
        modified = str(payload.get("modified") or now_tw)
        content_type = str(payload.get("type") or DEFAULT_TIDDLER_TYPE)
        text = payload.get("text")
        extra_fields = {
            k: v for k, v in payload.items() if k not in CORE_TIDDLER_KEYS
        }
        canonical_uri = extra_fields.get("_canonical_uri", "")
        blob_hash = (
            canonical_uri.removeprefix(CANONICAL_URI_PREFIX)
            if isinstance(canonical_uri, str)
            and canonical_uri.startswith(CANONICAL_URI_PREFIX)
            else payload.get("blob_hash")
        )
        fields_json = json.dumps(extra_fields, sort_keys=True)

        with self._connect() as conn:
            rev_row = conn.execute(
                "SELECT COALESCE(MAX(revision), 0) + 1 AS next_rev FROM tiddlers"
            ).fetchone()
            next_rev = int(rev_row["next_rev"])
            conn.execute(
                """
                INSERT INTO tiddlers (
                    title, revision, modified, type, fields_json,
                    text, blob_hash, deleted, dirty
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0)
                ON CONFLICT(title) DO UPDATE SET
                    revision = excluded.revision,
                    modified = excluded.modified,
                    type = excluded.type,
                    fields_json = excluded.fields_json,
                    text = excluded.text,
                    blob_hash = excluded.blob_hash,
                    deleted = 0,
                    dirty = 0
                """,
                (
                    title,
                    next_rev,
                    modified,
                    content_type,
                    fields_json,
                    text,
                    blob_hash,
                ),
            )

        full_tiddler = {
            **extra_fields,
            "title": title,
            "revision": next_rev,
            "modified": modified,
            "type": content_type,
            "bag": "default",
        }
        if text is not None:
            full_tiddler["text"] = text
        self.write_tiddler_backup(title, full_tiddler, deleted=False)
        return next_rev

    def delete_tiddler(self, title: str) -> bool:
        """Soft-delete a tiddler via tombstone revision and update its backup."""
        now_tw = self._current_tw_timestamp()
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT deleted FROM tiddlers WHERE title = ?", (title,)
            ).fetchone()
            if existing is None or int(existing["deleted"]) == 1:
                return False

            rev_row = conn.execute(
                "SELECT COALESCE(MAX(revision), 0) + 1 AS next_rev FROM tiddlers"
            ).fetchone()
            next_rev = int(rev_row["next_rev"])
            conn.execute(
                """
                UPDATE tiddlers
                SET revision = ?, modified = ?, text = NULL, deleted = 1, dirty = 0
                WHERE title = ?
                """,
                (next_rev, now_tw, title),
            )

        self.write_tiddler_backup(
            title,
            {
                "title": title,
                "revision": next_rev,
                "modified": now_tw,
                "deleted": True,
            },
            deleted=True,
        )
        return True

    def get_blob(self, sha256_hash: str) -> tuple[str, bytes] | None:
        """Retrieve a binary blob and its MIME type by SHA-256 hash."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT mime_type, data FROM blobs WHERE sha256 = ?",
                (sha256_hash,),
            ).fetchone()
        if row is not None:
            return str(row["mime_type"]), bytes(row["data"])

        backup_file = self.blob_backup_dir / sha256_hash
        if backup_file.is_file():
            return "application/octet-stream", backup_file.read_bytes()
        return None

    def put_blob(
        self, expected_sha256: str, mime_type: str, data: bytes
    ) -> str:
        """Verify SHA-256 and store a binary blob in SQLite and backup disk."""
        actual_sha256 = hashlib.sha256(data).hexdigest()
        if expected_sha256 and expected_sha256 != actual_sha256:
            raise ValueError(
                f"SHA-256 mismatch: expected {expected_sha256}, got {actual_sha256}"
            )

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO blobs (sha256, mime_type, size_bytes, data)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(sha256) DO NOTHING
                """,
                (actual_sha256, mime_type, len(data), data),
            )

        self.write_blob_backup(actual_sha256, data)
        return actual_sha256

    def sync_batch(self, request_payload: dict[str, Any]) -> dict[str, Any]:
        """Apply client delta changes and return server changes since `sinceRevision`."""
        since_revision = int(request_payload.get("sinceRevision", 0))
        limit = int(request_payload.get("limit", 100))

        for tiddler in request_payload.get("upserted", []):
            title = tiddler.get("title")
            if title:
                self.put_tiddler(str(title), tiddler)

        for deleted_title in request_payload.get("deleted", []):
            self.delete_tiddler(str(deleted_title))

        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT title, revision, modified, type, fields_json, text, blob_hash, deleted
                FROM tiddlers
                WHERE revision > ?
                ORDER BY revision ASC
                LIMIT ?
                """,
                (since_revision, limit),
            ).fetchall()
            max_rev_row = conn.execute(
                "SELECT COALESCE(MAX(revision), 0) AS current_rev FROM tiddlers"
            ).fetchone()
            current_rev = int(max_rev_row["current_rev"])

        changed: list[dict[str, Any]] = []
        deleted: list[str] = []
        for row in rows:
            if int(row["deleted"]) == 1:
                deleted.append(str(row["title"]))
            else:
                changed.append(self._row_to_tiddler(row, include_text=True))

        return {
            "currentRevision": current_rev,
            "changed": changed,
            "deleted": deleted,
        }

    def write_tiddler_backup(
        self, title: str, tiddler_obj: dict[str, Any], deleted: bool = False
    ) -> Path:
        """Write an atomic human-readable JSON snapshot of a tiddler to backup/tiddlers/."""
        safe_name = urllib.parse.quote(title, safe="")
        active_path = self.tiddler_backup_dir / f"{safe_name}.json"
        deleted_path = self.tiddler_backup_dir / f"{safe_name}.deleted.json"

        target_path = deleted_path if deleted else active_path
        stale_path = active_path if deleted else deleted_path
        if stale_path.exists():
            stale_path.unlink()

        temp_path = target_path.with_suffix(".tmp")
        temp_path.write_text(
            json.dumps(tiddler_obj, indent=2, sort_keys=True, ensure_ascii=False)
            + "\n",
            encoding="utf-8",
        )
        temp_path.replace(target_path)
        return target_path

    def write_blob_backup(self, sha256_hash: str, data: bytes) -> Path:
        """Write an atomic raw binary file to backup/blobs/<sha256>."""
        target_path = self.blob_backup_dir / sha256_hash
        if not target_path.exists():
            temp_path = target_path.with_suffix(".tmp")
            temp_path.write_bytes(data)
            temp_path.replace(target_path)
        return target_path

    @staticmethod
    def _row_to_tiddler(
        row: sqlite3.Row, include_text: bool
    ) -> dict[str, Any]:
        fields = json.loads(row["fields_json"] or "{}")
        result: dict[str, Any] = {
            **fields,
            "title": row["title"],
            "revision": int(row["revision"]),
            "modified": row["modified"],
            "type": row["type"],
            "bag": "default",
        }
        if row["blob_hash"] and "_canonical_uri" not in result:
            result["_canonical_uri"] = f"files/{row['blob_hash']}"
        if include_text and row["text"] is not None:
            result["text"] = row["text"]
        return result

    @staticmethod
    def _current_tw_timestamp() -> str:
        return datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y%m%d%H%M%S%f"
        )[:17]


class TiddlyRequestHandler(BaseHTTPRequestHandler):
    """HTTP handler wiring tiddlyweb, blob, and batch sync routes to TiddlerStore."""

    store: TiddlerStore

    def do_GET(self) -> None:  # pylint: disable=invalid-name
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if path == "/status":
            self._send_json(HTTPStatus.OK, self.store.get_status())
            return

        if path == "/recipes/default/tiddlers.json":
            self._send_json(HTTPStatus.OK, self.store.list_skinny_tiddlers())
            return

        if path.startswith("/recipes/default/tiddlers/"):
            title = urllib.parse.unquote(
                path.removeprefix("/recipes/default/tiddlers/")
            )
            tiddler = self.store.get_tiddler(title)
            if tiddler is None:
                self._send_json(HTTPStatus.NOT_FOUND, {"error": "Not found"})
            else:
                self._send_json(HTTPStatus.OK, tiddler)
            return

        if path.startswith("/files/"):
            sha256_hash = urllib.parse.unquote(path.removeprefix("/files/"))
            blob = self.store.get_blob(sha256_hash)
            if blob is None:
                self._send_json(HTTPStatus.NOT_FOUND, {"error": "Blob not found"})
            else:
                mime_type, data = blob
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", mime_type)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            return

        self._send_json(HTTPStatus.NOT_FOUND, {"error": "Route not found"})

    def do_PUT(self) -> None:  # pylint: disable=invalid-name
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        body = self._read_body()

        if path.startswith("/recipes/default/tiddlers/"):
            title = urllib.parse.unquote(
                path.removeprefix("/recipes/default/tiddlers/")
            )
            payload = json.loads(body.decode("utf-8"))
            rev = self.store.put_tiddler(title, payload)
            encoded_title = urllib.parse.quote(title, safe="")
            etag = f'"default/{encoded_title}/{rev}:"'
            self.send_response(HTTPStatus.NO_CONTENT)
            self.send_header("ETag", etag)
            self.end_headers()
            return

        if path.startswith("/files/"):
            sha256_hash = urllib.parse.unquote(path.removeprefix("/files/"))
            mime_type = self.headers.get(
                "Content-Type", "application/octet-stream"
            )
            try:
                stored_hash = self.store.put_blob(sha256_hash, mime_type, body)
            except ValueError as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            self._send_json(
                HTTPStatus.CREATED,
                {"sha256": stored_hash, "uri": f"files/{stored_hash}"},
            )
            return

        self._send_json(HTTPStatus.NOT_FOUND, {"error": "Route not found"})

    def do_DELETE(self) -> None:  # pylint: disable=invalid-name
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if path.startswith("/bags/default/tiddlers/"):
            title = urllib.parse.unquote(
                path.removeprefix("/bags/default/tiddlers/")
            )
            deleted = self.store.delete_tiddler(title)
            if not deleted:
                self._send_json(HTTPStatus.NOT_FOUND, {"error": "Not found"})
                return
            self.send_response(HTTPStatus.NO_CONTENT)
            self.end_headers()
            return

        self._send_json(HTTPStatus.NOT_FOUND, {"error": "Route not found"})

    def do_POST(self) -> None:  # pylint: disable=invalid-name
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/sync":
            payload = json.loads(self._read_body().decode("utf-8"))
            response = self.store.sync_batch(payload)
            self._send_json(HTTPStatus.OK, response)
            return

        self._send_json(HTTPStatus.NOT_FOUND, {"error": "Route not found"})

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length > 0 else b""
        if self.headers.get("Content-Encoding", "").lower() == "gzip":
            return gzip.decompress(raw)
        return raw

    def _send_json(self, status: HTTPStatus, payload: Any) -> None:
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        accept_encoding = self.headers.get("Accept-Encoding", "")
        use_gzip = "gzip" in accept_encoding and len(raw) > 512
        body = gzip.compress(raw) if use_gzip else raw

        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        if use_gzip:
            self.send_header("Content-Encoding", "gzip")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:  # pylint: disable=redefined-builtin
        if os.environ.get("QUIET_LOGS") == "1":
            return
        super().log_message(format, *args)


def run_server(port: int = 8080, data_dir: Path = DEFAULT_DATA_DIR) -> None:
    """Start the threaded TiddlyWiki sync server."""
    store = TiddlerStore(data_dir=data_dir)
    TiddlyRequestHandler.store = store
    with ThreadingHTTPServer(("0.0.0.0", port), TiddlyRequestHandler) as httpd:
        print(f"Python TiddlyWiki server listening on http://localhost:{port}")
        httpd.serve_forever()


if __name__ == "__main__":
    run_server(port=int(os.environ.get("PORT", "8080")))
