# Hosted TiddlyWiki: Standalone Android App & Multi-Language Tiddler Sync Backends

A standalone Android application and four interchangeable, minimal tiddler-syncing backends (**JavaScript / Node.js**, **Java / Javalin**, **Go**, and **Python**) implementing TiddlyWiki's `tiddlyweb` sync protocol, content-addressed binary blob streaming (`_canonical_uri`), and periodic delta batch synchronization.

## Dual-Storage Architecture (`data/`)

Both the Android app and every backend server store tiddlers and binary blobs using a dual-storage layout inside `data/`:

1. **SQLite (`data/tiddlywiki.db`)** — Primary indexed store for sub-millisecond reads/writes, skinny-tiddler listing (metadata without heavy `text`), and revision-based delta queries.
2. **Human-Readable JSON & Binary File Backups (`data/backup/`)**:
   - `data/backup/tiddlers/<url_encoded_title>.json` — Full JSON snapshot of each active tiddler (plus `<url_encoded_title>.deleted.json` tombstones for deleted tiddlers) for backup and git/filesystem visibility.
   - `data/backup/blobs/<sha256>` — Raw content-addressed binary attachments (`>100 KB` referenced via `_canonical_uri: "files/<sha256>"`).

> **Branching Policy**: The `data/` directory is ignored on `main` and committed exclusively to the `private_data` branch (branched off `main`), with its own `data/revisions.txt` history.

## Aligned Backend API & Method Naming

All four servers expose identical HTTP endpoints and align their method names using each language's readability conventions:

| Endpoint | Purpose | JS (`camelCase`) | Java (`camelCase`) | Go (`PascalCase`) | Python (`snake_case`) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| *(Startup)* | Initialize SQLite & backup folders | `initStorage` | `initStorage` | `InitStorage` | `init_storage` |
| `GET /status` | `tiddlyweb` server status | `getStatus` | `getStatus` | `GetStatus` | `get_status` |
| `GET /recipes/default/tiddlers.json` | List skinny tiddlers (omit `text`) | `listSkinnyTiddlers` | `listSkinnyTiddlers` | `ListSkinnyTiddlers` | `list_skinny_tiddlers` |
| `GET /recipes/default/tiddlers/:title` | Fetch single full tiddler | `getTiddler` | `getTiddler` | `GetTiddler` | `get_tiddler` |
| `PUT /recipes/default/tiddlers/:title` | Upsert tiddler + write JSON backup | `putTiddler` | `putTiddler` | `PutTiddler` | `put_tiddler` |
| `DELETE /bags/default/tiddlers/:title` | Tombstone delete + update backup | `deleteTiddler` | `deleteTiddler` | `DeleteTiddler` | `delete_tiddler` |
| `GET /files/:sha256` | Stream content-addressed blob | `getBlob` | `getBlob` | `GetBlob` | `get_blob` |
| `PUT /files/:sha256` | Store content-addressed blob | `putBlob` | `putBlob` | `PutBlob` | `put_blob` |
| `POST /api/sync` | Paginated delta batch sync | `syncBatch` | `syncBatch` | `SyncBatch` | `sync_batch` |
| *(Helper)* | Write human-readable JSON backup | `writeTiddlerBackup` | `writeTiddlerBackup` | `WriteTiddlerBackup` | `write_tiddler_backup` |
| *(Helper)* | Write raw binary blob backup | `writeBlobBackup` | `writeBlobBackup` | `WriteBlobBackup` | `write_blob_backup` |

See [`docs/BACKEND_COMPARISON.md`](docs/BACKEND_COMPARISON.md) for an analysis of how concurrency, SQLite bindings, dynamic JSON field serialization, and binary streaming differ across the four languages.

## Running the Backends

All backends default to port `8080` and `DATA_DIR=../../data` (or `./data` from repository root):

- **Python 3 (Zero external dependencies)**:
  ```sh
  python3 backends/python/server.py
  # Run automated integration tests:
  python3 backends/python/test_server.py
  ```
- **Node.js (Node 22.5+ built-in `node:sqlite`)**:
  ```sh
  node backends/js/server.js
  ```
- **Go (Go 1.22+)**:
  ```sh
  cd backends/go && go run main.go
  ```
- **Java (Java 21+ with Javalin 6)**:
  ```sh
  cd backends/java && gradle run
  ```
