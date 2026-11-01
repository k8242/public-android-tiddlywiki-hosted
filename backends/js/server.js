/**
 * Minimal TiddlyWiki tiddler-syncing backend in Node.js (ES Modules).
 *
 * Uses Node 22.5+ built-in `node:http`, `node:sqlite` (`DatabaseSync`),
 * `node:crypto`, and `node:zlib` with zero external npm dependencies.
 *
 * Storage layout under DATA_DIR (defaults to <repo_root>/data):
 *   - tiddlywiki.db: SQLite database (WAL mode) with `tiddlers` and `blobs`
 *   - backup/tiddlers/<encoded_title>.json: Human-readable JSON snapshot
 *   - backup/blobs/<sha256>: Raw binary file backup
 */

import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { fileURLToPath } from 'node:url';
import zlib from 'node:zlib';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, '..', '..');
const DEFAULT_DATA_DIR = process.env.DATA_DIR || path.join(REPO_ROOT, 'data');
const SCHEMA_PATH = path.resolve(__dirname, '..', 'schema.sql');

const CORE_TIDDLER_KEYS = new Set([
  'title',
  'revision',
  'modified',
  'type',
  'text',
  'bag',
]);

export class TiddlerStore {
  constructor(dataDir = DEFAULT_DATA_DIR) {
    this.dataDir = dataDir;
    this.dbPath = path.join(this.dataDir, 'tiddlywiki.db');
    this.tiddlerBackupDir = path.join(this.dataDir, 'backup', 'tiddlers');
    this.blobBackupDir = path.join(this.dataDir, 'backup', 'blobs');
    this.db = null;
    this.initStorage();
  }

  /** Initialize SQLite WAL database and backup directories. */
  initStorage() {
    fs.mkdirSync(this.tiddlerBackupDir, { recursive: true });
    fs.mkdirSync(this.blobBackupDir, { recursive: true });
    this.db = new DatabaseSync(this.dbPath);
    const schemaSql = fs.readFileSync(SCHEMA_PATH, 'utf8');
    this.db.exec(schemaSql);
  }

  /** Return tiddlyweb server status metadata. */
  getStatus() {
    return {
      username: 'user',
      space: { recipe: 'default' },
      tiddlywiki_version: '5.3.5',
    };
  }

  /** Return all non-deleted tiddlers omitting the heavy `text` field. */
  listSkinnyTiddlers() {
    const stmt = this.db.prepare(`
      SELECT title, revision, modified, type, fields_json, blob_hash
      FROM tiddlers
      WHERE deleted = 0
      ORDER BY modified DESC
    `);
    return stmt.all().map((row) => this.#rowToTiddler(row, false));
  }

  /** Fetch a single full tiddler (including `text`) by title. */
  getTiddler(title) {
    const stmt = this.db.prepare(`
      SELECT title, revision, modified, type, fields_json, text, blob_hash
      FROM tiddlers
      WHERE title = ? AND deleted = 0
    `);
    const row = stmt.get(title);
    return row ? this.#rowToTiddler(row, true) : null;
  }

  /** Upsert a tiddler in SQLite and write its human-readable JSON backup. */
  putTiddler(title, payload) {
    const modified = String(payload.modified || this.#currentTwTimestamp());
    const type = String(payload.type || 'text/vnd.tiddlywiki');
    const text = payload.text ?? null;

    const extraFields = {};
    for (const [key, value] of Object.entries(payload)) {
      if (!CORE_TIDDLER_KEYS.has(key)) {
        extraFields[key] = value;
      }
    }

    const canonicalUri = extraFields._canonical_uri || '';
    const blobHash =
      typeof canonicalUri === 'string' && canonicalUri.startsWith('files/')
        ? canonicalUri.slice('files/'.length)
        : (payload.blob_hash ?? null);
    const fieldsJson = JSON.stringify(extraFields);

    const revRow = this.db
      .prepare('SELECT COALESCE(MAX(revision), 0) + 1 AS next_rev FROM tiddlers')
      .get();
    const nextRev = Number(revRow.next_rev);

    this.db
      .prepare(
        `
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
      `
      )
      .run(title, nextRev, modified, type, fieldsJson, text, blobHash);

    const fullTiddler = {
      ...extraFields,
      title,
      revision: nextRev,
      modified,
      type,
      bag: 'default',
      ...(text !== null ? { text } : {}),
    };
    this.writeTiddlerBackup(title, fullTiddler, false);
    return nextRev;
  }

  /** Soft-delete a tiddler via tombstone revision and update its backup. */
  deleteTiddler(title) {
    const existing = this.db
      .prepare('SELECT deleted FROM tiddlers WHERE title = ?')
      .get(title);
    if (!existing || Number(existing.deleted) === 1) {
      return false;
    }

    const nowTw = this.#currentTwTimestamp();
    const revRow = this.db
      .prepare('SELECT COALESCE(MAX(revision), 0) + 1 AS next_rev FROM tiddlers')
      .get();
    const nextRev = Number(revRow.next_rev);

    this.db
      .prepare(
        `
        UPDATE tiddlers
        SET revision = ?, modified = ?, text = NULL, deleted = 1, dirty = 0
        WHERE title = ?
      `
      )
      .run(nextRev, nowTw, title);

    this.writeTiddlerBackup(
      title,
      { title, revision: nextRev, modified: nowTw, deleted: true },
      true
    );
    return true;
  }

  /** Retrieve a binary blob and its MIME type by SHA-256 hash. */
  getBlob(sha256Hash) {
    const row = this.db
      .prepare('SELECT mime_type, data FROM blobs WHERE sha256 = ?')
      .get(sha256Hash);
    if (row) {
      return { mimeType: row.mime_type, data: Buffer.from(row.data) };
    }

    const backupFile = path.join(this.blobBackupDir, sha256Hash);
    if (fs.existsSync(backupFile)) {
      return {
        mimeType: 'application/octet-stream',
        data: fs.readFileSync(backupFile),
      };
    }
    return null;
  }

  /** Verify SHA-256 and store a binary blob in SQLite and backup disk. */
  putBlob(expectedSha256, mimeType, dataBuffer) {
    const actualSha256 = crypto
      .createHash('sha256')
      .update(dataBuffer)
      .digest('hex');
    if (expectedSha256 && expectedSha256 !== actualSha256) {
      throw new Error(
        `SHA-256 mismatch: expected ${expectedSha256}, got ${actualSha256}`
      );
    }

    this.db
      .prepare(
        `
        INSERT INTO blobs (sha256, mime_type, size_bytes, data)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(sha256) DO NOTHING
      `
      )
      .run(actualSha256, mimeType, dataBuffer.length, dataBuffer);

    this.writeBlobBackup(actualSha256, dataBuffer);
    return actualSha256;
  }

  /** Apply client delta changes and return server changes since `sinceRevision`. */
  syncBatch(requestPayload) {
    const sinceRevision = Number(requestPayload.sinceRevision ?? 0);
    const limit = Number(requestPayload.limit ?? 100);

    for (const tiddler of requestPayload.upserted ?? []) {
      if (tiddler?.title) {
        this.putTiddler(String(tiddler.title), tiddler);
      }
    }
    for (const deletedTitle of requestPayload.deleted ?? []) {
      this.deleteTiddler(String(deletedTitle));
    }

    const rows = this.db
      .prepare(
        `
        SELECT title, revision, modified, type, fields_json, text, blob_hash, deleted
        FROM tiddlers
        WHERE revision > ?
        ORDER BY revision ASC
        LIMIT ?
      `
      )
      .all(sinceRevision, limit);

    const maxRevRow = this.db
      .prepare('SELECT COALESCE(MAX(revision), 0) AS current_rev FROM tiddlers')
      .get();

    const changed = [];
    const deleted = [];
    for (const row of rows) {
      if (Number(row.deleted) === 1) {
        deleted.push(row.title);
      } else {
        changed.push(this.#rowToTiddler(row, true));
      }
    }

    return {
      currentRevision: Number(maxRevRow.current_rev),
      changed,
      deleted,
    };
  }

  /** Write an atomic human-readable JSON snapshot of a tiddler to backup/tiddlers/. */
  writeTiddlerBackup(title, tiddlerObj, deleted = false) {
    const safeName = encodeURIComponent(title);
    const activePath = path.join(this.tiddlerBackupDir, `${safeName}.json`);
    const deletedPath = path.join(
      this.tiddlerBackupDir,
      `${safeName}.deleted.json`
    );

    const targetPath = deleted ? deletedPath : activePath;
    const stalePath = deleted ? activePath : deletedPath;
    if (fs.existsSync(stalePath)) {
      fs.unlinkSync(stalePath);
    }

    const tempPath = `${targetPath}.tmp`;
    fs.writeFileSync(tempPath, `${JSON.stringify(tiddlerObj, null, 2)}\n`, 'utf8');
    fs.renameSync(tempPath, targetPath);
    return targetPath;
  }

  /** Write an atomic raw binary file to backup/blobs/<sha256>. */
  writeBlobBackup(sha256Hash, dataBuffer) {
    const targetPath = path.join(this.blobBackupDir, sha256Hash);
    if (!fs.existsSync(targetPath)) {
      const tempPath = `${targetPath}.tmp`;
      fs.writeFileSync(tempPath, dataBuffer);
      fs.renameSync(tempPath, targetPath);
    }
    return targetPath;
  }

  #rowToTiddler(row, includeText) {
    const fields = JSON.parse(row.fields_json || '{}');
    const result = {
      ...fields,
      title: row.title,
      revision: Number(row.revision),
      modified: row.modified,
      type: row.type,
      bag: 'default',
    };
    if (row.blob_hash && !('_canonical_uri' in result)) {
      result._canonical_uri = `files/${row.blob_hash}`;
    }
    if (includeText && row.text !== null && row.text !== undefined) {
      result.text = row.text;
    }
    return result;
  }

  #currentTwTimestamp() {
    return new Date().toISOString().replace(/[-T:.Z]/g, '').slice(0, 17);
  }
}

function readRequestBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', (chunk) => chunks.push(chunk));
    req.on('end', () => {
      const raw = Buffer.concat(chunks);
      if ((req.headers['content-encoding'] || '').toLowerCase() === 'gzip') {
        try {
          resolve(zlib.gunzipSync(raw));
        } catch (err) {
          reject(err);
        }
      } else {
        resolve(raw);
      }
    });
    req.on('error', reject);
  });
}

function sendJson(req, res, statusCode, payload) {
  const raw = Buffer.from(JSON.stringify(payload), 'utf8');
  const acceptEncoding = req.headers['accept-encoding'] || '';
  const useGzip = acceptEncoding.includes('gzip') && raw.length > 512;
  const body = useGzip ? zlib.gzipSync(raw) : raw;

  const headers = {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': String(body.length),
  };
  if (useGzip) {
    headers['Content-Encoding'] = 'gzip';
  }
  res.writeHead(statusCode, headers);
  res.end(body);
}

export function createServer(store = new TiddlerStore()) {
  return http.createServer(async (req, res) => {
    const url = new URL(req.url || '/', 'http://localhost');
    const pathname = url.pathname;

    try {
      if (req.method === 'GET' && pathname === '/status') {
        sendJson(req, res, 200, store.getStatus());
        return;
      }

      if (
        req.method === 'GET' &&
        pathname === '/recipes/default/tiddlers.json'
      ) {
        sendJson(req, res, 200, store.listSkinnyTiddlers());
        return;
      }

      if (pathname.startsWith('/recipes/default/tiddlers/')) {
        const title = decodeURIComponent(
          pathname.slice('/recipes/default/tiddlers/'.length)
        );
        if (req.method === 'GET') {
          const tiddler = store.getTiddler(title);
          if (!tiddler) {
            sendJson(req, res, 404, { error: 'Not found' });
          } else {
            sendJson(req, res, 200, tiddler);
          }
          return;
        }
        if (req.method === 'PUT') {
          const body = await readRequestBody(req);
          const payload = JSON.parse(body.toString('utf8'));
          const rev = store.putTiddler(title, payload);
          const etag = `"default/${encodeURIComponent(title)}/${rev}:"`;
          res.writeHead(204, { ETag: etag });
          res.end();
          return;
        }
      }

      if (
        req.method === 'DELETE' &&
        pathname.startsWith('/bags/default/tiddlers/')
      ) {
        const title = decodeURIComponent(
          pathname.slice('/bags/default/tiddlers/'.length)
        );
        const deleted = store.deleteTiddler(title);
        if (!deleted) {
          sendJson(req, res, 404, { error: 'Not found' });
        } else {
          res.writeHead(204);
          res.end();
        }
        return;
      }

      if (pathname.startsWith('/files/')) {
        const sha256Hash = decodeURIComponent(pathname.slice('/files/'.length));
        if (req.method === 'GET') {
          const blob = store.getBlob(sha256Hash);
          if (!blob) {
            sendJson(req, res, 404, { error: 'Blob not found' });
          } else {
            res.writeHead(200, {
              'Content-Type': blob.mimeType,
              'Content-Length': String(blob.data.length),
            });
            res.end(blob.data);
          }
          return;
        }
        if (req.method === 'PUT') {
          const body = await readRequestBody(req);
          const mimeType =
            req.headers['content-type'] || 'application/octet-stream';
          const storedHash = store.putBlob(sha256Hash, mimeType, body);
          sendJson(req, res, 201, {
            sha256: storedHash,
            uri: `files/${storedHash}`,
          });
          return;
        }
      }

      if (req.method === 'POST' && pathname === '/api/sync') {
        const body = await readRequestBody(req);
        const payload = JSON.parse(body.toString('utf8'));
        sendJson(req, res, 200, store.syncBatch(payload));
        return;
      }

      sendJson(req, res, 404, { error: 'Route not found' });
    } catch (err) {
      sendJson(req, res, 400, { error: err.message || 'Bad request' });
    }
  });
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const port = Number(process.env.PORT || 8080);
  const server = createServer();
  server.listen(port, () => {
    console.log(`Node.js TiddlyWiki server listening on http://localhost:${port}`);
  });
}
