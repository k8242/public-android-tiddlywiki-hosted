# Backend Comparison: Node.js vs. Java (Javalin) vs. Go vs. Python

All four backend servers (`backends/js/server.js`, `backends/java/.../TiddlyServer.java`, `backends/go/main.go`, and `backends/python/server.py`) implement the exact same TiddlyWiki `tiddlyweb` + `/files/{sha256}` + `/api/sync` HTTP contract and dual SQLite/filesystem backup storage.

Below is an analysis of the functionality that behaves **significantly differently** across the four languages and runtimes.

---

## 1. Concurrency Model & SQLite Write Serialization

SQLite in `WAL` (Write-Ahead Logging) mode allows **unlimited concurrent readers** alongside **a single writer**. How each backend coordinates concurrent HTTP requests against SQLite differs fundamentally:

| Backend | HTTP Concurrency Model | How SQLite Writes Are Serialized |
| :--- | :--- | :--- |
| **Node.js (`server.js`)** | **Single-threaded event loop** (`node:http`). Request bodies are awaited asynchronously, while `DatabaseSync` (`node:sqlite`) executes synchronously on the main JS thread. | **Implicitly serialized**: Because `putTiddler`, `deleteTiddler`, and `putBlob` execute synchronously inside a single event-loop tick, two writes can never interleave or trigger `SQLITE_BUSY` without any locks. |
| **Go (`main.go`)** | **Goroutine-per-request** (`net/http`). Hundreds of requests execute in parallel across OS threads via the Go scheduler, sharing a single `*sql.DB` connection pool. | **Explicit `sync.Mutex` (`writeMu`)**: Because `database/sql` opens multiple pooled connections, concurrent goroutines running `SELECT COALESCE(MAX(revision), 0) + 1` followed by `INSERT` would race or hit `SQLITE_BUSY`. Go requires an explicit `sync.Mutex` (or `BEGIN IMMEDIATE` transactions) around write operations. |
| **Java (`TiddlyServer.java`)** | **Thread pool / Virtual threads** (Jetty inside Javalin 6). Each request runs on its own worker thread. | **`synchronized (writeLock)` block**: Similar to Go, multi-threaded JDBC connections require an explicit `synchronized (writeLock)` monitor so revision allocation + SQLite `INSERT` + JSON backup file replacement remain strictly ordered. |
| **Python (`server.py`)** | **OS thread per request** (`ThreadingHTTPServer`). Each request opens a short-lived `sqlite3.connect(..., timeout=10.0)` handle. | **SQLite C-level busy timeout + GIL**: Python's `sqlite3` module automatically wraps DML statements in transactions and waits up to `timeout=10.0` seconds on the OS file lock when another thread is writing. |

---

## 2. Dynamic TiddlyWiki JSON Field Handling

A TiddlyWiki tiddler is a flat JSON object that mixes **schema-known fields** (`title`, `revision`, `modified`, `type`, `text`, `bag`) with **arbitrary user-defined fields** (`tags`, `_canonical_uri`, `caption`, `list`, custom metadata) at the same top-level JSON root.

- **JavaScript & Python (Native Dictionary Spread / Comprehensions)**:
  Dynamic key separation and reconstruction takes 2–3 lines:
  ```js
  // JS: Spread extra fields directly into the top-level response object
  const fullTiddler = { ...extraFields, title, revision: nextRev, modified, type, bag: 'default' };
  ```
  ```python
  # Python: Dict comprehension + dictionary unpacking
  extra_fields = {k: v for k, v in payload.items() if k not in CORE_TIDDLER_KEYS}
  full_tiddler = {**extra_fields, "title": title, "revision": next_rev, ...}
  ```
- **Go (`map[string]any` Type Assertions)**:
  Because Go is statically typed and `encoding/json` unmarshals untyped JSON objects into `map[string]any` (where all numbers become `float64`), Go cannot use a single strongly-typed `struct` for a tiddler without losing arbitrary top-level fields (unless writing a custom `json.Unmarshaler`/`json.Marshaler`). Instead, every field access requires explicit type assertions (`payload["text"].(string)`), and maps must be copied loop-by-loop.
- **Java (`LinkedHashMap<String, Object>` + Jackson `TypeReference`)**:
  Java requires Jackson's `new TypeReference<LinkedHashMap<String, Object>>() {}` to preserve generic type information at runtime (due to JVM type erasure) and explicit casts (`(List<Map<String, Object>>) requestPayload.get("upserted")`) when traversing nested payloads in `syncBatch`.

---

## 3. SQLite Driver Architecture & Packaging

| Backend | SQLite Library | Native / C Dependency Tradeoff |
| :--- | :--- | :--- |
| **Python** | `sqlite3` (Standard Library) | Built into Python's standard library out of the box; zero external packages required. |
| **Node.js** | `node:sqlite` (Built-in in Node 22.5+) | Bundled directly inside the Node.js binary via `DatabaseSync`, eliminating native node-gyp compilation (`better-sqlite3` / `sqlite3`). |
| **Go** | `modernc.org/sqlite` (`database/sql`) | Go's standard library includes `database/sql` interfaces, not a driver. Using `modernc.org/sqlite` (a C-to-Go transpilation of SQLite) avoids `CGO` so the server still cross-compiles to a single static binary. |
| **Java** | `org.xerial:sqlite-jdbc` | Bundles precompiled native `.dylib`/`.so`/`.dll` SQLite binaries inside the JAR and extracts them to `/tmp` on JVM startup. Requires verbose `try (Connection ...; PreparedStatement ...; ResultSet ...)` blocks. |

---

## 4. HTTP Compression & Binary Blob Handling (`>1 MB`)

- **Javalin (Java)** provides built-in Jetty Gzip negotiation (`config.http.gzipOnlyCompression()`) and automatic `PathParam` URL decoding, making route definitions the most concise of the four backends.
- **Go (`net/http`)**, **Node.js (`node:http`)**, and **Python (`http.server`)** do not automatically compress responses or decompress `Content-Encoding: gzip` request bodies in the standard library; each server explicitly checks `Accept-Encoding: gzip` (for payloads `>512` bytes) and `Content-Encoding: gzip` using `compress/gzip`, `node:zlib`, and `gzip` respectively.
- **Atomic File Backups (`writeTiddlerBackup` / `writeBlobBackup`)**:
  - In **Java**, `Files.move(tempPath, targetPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)` explicitly enforces OS-level atomic rename semantics and throws `AtomicMoveNotSupportedException` if the filesystem cannot guarantee atomicity.
  - In **Go** (`os.Rename`), **Python** (`Path.replace`), and **Node.js** (`fs.renameSync`), POSIX `rename(2)` is atomic when `.tmp` is in the same directory, which all four implementations guarantee by creating `<filename>.tmp` in the target backup folder.
