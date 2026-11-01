-- Shared SQLite schema for Hosted TiddlyWiki Sync Backends (JS, Java, Go, Python)
-- and the standalone Android client database.

PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;

-- 1. Tiddlers metadata and text content
-- Large binary attachments (>100 KB) use _canonical_uri in fields_json and store
-- their SHA-256 pointer in blob_hash while keeping text NULL.
CREATE TABLE IF NOT EXISTS tiddlers (
    title TEXT PRIMARY KEY,
    revision INTEGER NOT NULL,
    modified TEXT NOT NULL,
    type TEXT NOT NULL DEFAULT 'text/vnd.tiddlywiki',
    fields_json TEXT NOT NULL DEFAULT '{}',
    text TEXT,
    blob_hash TEXT,
    deleted INTEGER NOT NULL DEFAULT 0,
    dirty INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_tiddlers_revision ON tiddlers(revision);
CREATE INDEX IF NOT EXISTS idx_tiddlers_dirty ON tiddlers(dirty) WHERE dirty = 1;

-- 2. Content-addressed binary blobs (for >100 KB attachments and media)
CREATE TABLE IF NOT EXISTS blobs (
    sha256 TEXT PRIMARY KEY,
    mime_type TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    data BLOB NOT NULL
);
