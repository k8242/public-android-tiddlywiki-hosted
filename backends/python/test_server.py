#!/usr/bin/env python3
"""Integration tests for the Python TiddlyWiki sync server and dual storage."""

from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest

from server import TiddlyRequestHandler, TiddlerStore


class _InMemorySocket:
    """In-memory socket wrapper allowing BaseHTTPRequestHandler testing in sandbox."""

    def __init__(self, request_bytes: bytes) -> None:
        self._rfile = io.BytesIO(request_bytes)
        self.wfile = io.BytesIO()

    def makefile(self, mode: str, *args: object, **kwargs: object) -> io.BytesIO:
        del args, kwargs
        if "r" in mode:
            return self._rfile
        return self.wfile

    def sendall(self, data: bytes) -> None:
        self.wfile.write(data)

    def close(self) -> None:
        pass


class TiddlyServerIntegrationTest(unittest.TestCase):
    """Verifies tiddlyweb endpoints, blob storage, batch sync, and file backups."""

    def setUp(self) -> None:
        os.environ["QUIET_LOGS"] = "1"
        self.temp_dir = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.temp_dir.name)
        self.store = TiddlerStore(data_dir=self.data_dir)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, dict[str, str], bytes]:
        hdrs = dict(headers or {})
        payload = body or b""
        if payload:
            hdrs["Content-Length"] = str(len(payload))
        header_lines = "".join(f"{k}: {v}\r\n" for k, v in hdrs.items())
        raw_req = (
            f"{method} {path} HTTP/1.1\r\nHost: localhost\r\n{header_lines}\r\n"
        ).encode("utf-8") + payload

        sock = _InMemorySocket(raw_req)

        class BoundHandler(TiddlyRequestHandler):
            pass

        BoundHandler.store = self.store
        BoundHandler(sock, ("127.0.0.1", 0), None)  # type: ignore[arg-type]

        raw_resp = sock.wfile.getvalue()
        head_bytes, _, resp_body = raw_resp.partition(b"\r\n\r\n")
        head_lines = head_bytes.decode("iso-8859-1").split("\r\n")
        status_code = int(head_lines[0].split(" ")[1])
        resp_headers: dict[str, str] = {}
        for line in head_lines[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                resp_headers[k.strip()] = v.strip()
        return status_code, resp_headers, resp_body

    def test_tiddler_lifecycle_and_backups(self) -> None:
        status_code, _, raw = self._request("GET", "/status")
        self.assertEqual(status_code, 200)
        self.assertEqual(json.loads(raw)["space"]["recipe"], "default")

        tiddler_payload = {
            "title": "GettingStarted",
            "text": "Welcome to Hosted TiddlyWiki!",
            "tags": "welcome [[core note]]",
            "modified": "20261101140000000",
        }
        status_code, headers, _ = self._request(
            "PUT",
            "/recipes/default/tiddlers/GettingStarted",
            body=json.dumps(tiddler_payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(status_code, 204)
        self.assertIn("default/GettingStarted/1:", headers.get("ETag", ""))

        # Verify JSON backup file on disk
        backup_file = (
            self.data_dir / "backup" / "tiddlers" / "GettingStarted.json"
        )
        self.assertTrue(backup_file.is_file())
        backup_data = json.loads(backup_file.read_text(encoding="utf-8"))
        self.assertEqual(backup_data["text"], "Welcome to Hosted TiddlyWiki!")

        # Verify skinny list omits `text`
        _, _, skinny_raw = self._request("GET", "/recipes/default/tiddlers.json")
        skinny_list = json.loads(skinny_raw)
        self.assertEqual(len(skinny_list), 1)
        self.assertNotIn("text", skinny_list[0])
        self.assertEqual(skinny_list[0]["tags"], "welcome [[core note]]")

        # Verify full tiddler includes `text`
        _, _, full_raw = self._request(
            "GET", "/recipes/default/tiddlers/GettingStarted"
        )
        full_tiddler = json.loads(full_raw)
        self.assertEqual(full_tiddler["text"], "Welcome to Hosted TiddlyWiki!")

        # Delete tiddler and verify tombstone backup
        del_status, _, _ = self._request(
            "DELETE", "/bags/default/tiddlers/GettingStarted"
        )
        self.assertEqual(del_status, 204)
        self.assertFalse(backup_file.exists())
        self.assertTrue(
            (
                self.data_dir
                / "backup"
                / "tiddlers"
                / "GettingStarted.deleted.json"
            ).is_file()
        )

    def test_blob_upload_and_batch_sync(self) -> None:
        blob_bytes = b"\x89PNG\r\n\x1a\n" + (b"A" * 2048)
        sha256_hash = hashlib.sha256(blob_bytes).hexdigest()

        status_code, _, resp_raw = self._request(
            "PUT",
            f"/files/{sha256_hash}",
            body=blob_bytes,
            headers={"Content-Type": "image/png"},
        )
        self.assertEqual(status_code, 201)
        self.assertEqual(json.loads(resp_raw)["uri"], f"files/{sha256_hash}")
        self.assertTrue(
            (self.data_dir / "backup" / "blobs" / sha256_hash).is_file()
        )

        # Verify GET /files/{sha256}
        get_status, get_headers, get_body = self._request(
            "GET", f"/files/{sha256_hash}"
        )
        self.assertEqual(get_status, 200)
        self.assertEqual(get_headers.get("Content-Type"), "image/png")
        self.assertEqual(get_body, blob_bytes)

        # Sync batch from Android client
        sync_req = {
            "sinceRevision": 0,
            "upserted": [
                {
                    "title": "ArchitectureDiagram.png",
                    "type": "image/png",
                    "_canonical_uri": f"files/{sha256_hash}",
                    "modified": "20261101140500000",
                }
            ],
            "deleted": [],
        }
        status_code, _, sync_raw = self._request(
            "POST",
            "/api/sync",
            body=json.dumps(sync_req).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(status_code, 200)
        sync_resp = json.loads(sync_raw)
        self.assertEqual(sync_resp["currentRevision"], 1)
        self.assertEqual(len(sync_resp["changed"]), 1)
        self.assertEqual(
            sync_resp["changed"][0]["_canonical_uri"], f"files/{sha256_hash}"
        )


if __name__ == "__main__":
    unittest.main()
