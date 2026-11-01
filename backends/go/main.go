// Package main provides a minimal TiddlyWiki tiddler-syncing backend in Go.
//
// It implements TiddlyWiki's tiddlyweb HTTP protocol, content-addressed binary
// blob storage (/files/{sha256}), and a batch delta sync endpoint (/api/sync)
// for the Android WorkManager periodic sync client.
package main

import (
	"compress/gzip"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const (
	defaultPort        = "8080"
	defaultBatchLimit  = 100
	defaultTiddlerType = "text/vnd.tiddlywiki"
	defaultBag         = "default"
	defaultRecipe      = "default"
	tiddlyWikiVersion  = "5.3.5"
	canonicalURIPrefix = "files/"
	gzipMinBytes       = 512
)

var coreTiddlerKeys = map[string]bool{
	"title":    true,
	"revision": true,
	"modified": true,
	"type":     true,
	"text":     true,
	"bag":      true,
}

// SyncRequest represents the JSON payload sent by Android WorkManager to POST /api/sync.
type SyncRequest struct {
	SinceRevision int              `json:"sinceRevision"`
	Limit         int              `json:"limit"`
	Upserted      []map[string]any `json:"upserted"`
	Deleted       []string         `json:"deleted"`
}

// SyncResponse represents the delta payload returned by POST /api/sync.
type SyncResponse struct {
	CurrentRevision int              `json:"currentRevision"`
	Changed         []map[string]any `json:"changed"`
	Deleted         []string         `json:"deleted"`
}

// TiddlerStore manages dual SQLite + filesystem backup storage for tiddlers and blobs.
type TiddlerStore struct {
	dataDir          string
	dbPath           string
	tiddlerBackupDir string
	blobBackupDir    string
	schemaPath       string
	db               *sql.DB
	writeMu          sync.Mutex
}

// NewTiddlerStore creates and initializes a TiddlerStore rooted at dataDir.
func NewTiddlerStore(dataDir, schemaPath string) (*TiddlerStore, error) {
	store := &TiddlerStore{
		dataDir:          dataDir,
		dbPath:           filepath.Join(dataDir, "tiddlywiki.db"),
		tiddlerBackupDir: filepath.Join(dataDir, "backup", "tiddlers"),
		blobBackupDir:    filepath.Join(dataDir, "backup", "blobs"),
		schemaPath:       schemaPath,
	}
	if err := store.InitStorage(); err != nil {
		return nil, err
	}
	return store, nil
}

// InitStorage initializes the SQLite WAL database and backup directories.
func (s *TiddlerStore) InitStorage() error {
	if err := os.MkdirAll(s.tiddlerBackupDir, 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(s.blobBackupDir, 0o755); err != nil {
		return err
	}

	db, err := sql.Open("sqlite", s.dbPath)
	if err != nil {
		return err
	}
	// Configure connection limits for SQLite WAL concurrency.
	db.SetMaxOpenConns(4)
	s.db = db

	schemaBytes, err := os.ReadFile(s.schemaPath)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(string(schemaBytes))
	return err
}

// GetStatus returns tiddlyweb server status metadata (aligned with getStatus / get_status).
func (s *TiddlerStore) GetStatus() map[string]any {
	return map[string]any{
		"username":           "user",
		"space":              map[string]any{"recipe": defaultRecipe},
		"tiddlywiki_version": tiddlyWikiVersion,
	}
}

// ListSkinnyTiddlers returns all non-deleted tiddlers omitting the heavy `text` field.
func (s *TiddlerStore) ListSkinnyTiddlers() ([]map[string]any, error) {
	rows, err := s.db.Query(`
		SELECT title, revision, modified, type, fields_json, COALESCE(blob_hash, '')
		FROM tiddlers
		WHERE deleted = 0
		ORDER BY modified DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]map[string]any, 0)
	for rows.Next() {
		var title, modified, contentType, fieldsJSON, blobHash string
		var revision int
		if err := rows.Scan(&title, &revision, &modified, &contentType, &fieldsJSON, &blobHash); err != nil {
			return nil, err
		}
		result = append(result, rowToTiddler(title, revision, modified, contentType, fieldsJSON, nil, blobHash, false))
	}
	return result, rows.Err()
}

// GetTiddler fetches a single full tiddler (including `text`) by title.
func (s *TiddlerStore) GetTiddler(title string) (map[string]any, error) {
	var revision int
	var modified, contentType, fieldsJSON, blobHash string
	var text sql.NullString

	err := s.db.QueryRow(`
		SELECT title, revision, modified, type, fields_json, text, COALESCE(blob_hash, '')
		FROM tiddlers
		WHERE title = ? AND deleted = 0
	`, title).Scan(&title, &revision, &modified, &contentType, &fieldsJSON, &text, &blobHash)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var textPtr *string
	if text.Valid {
		textPtr = &text.String
	}
	return rowToTiddler(title, revision, modified, contentType, fieldsJSON, textPtr, blobHash, true), nil
}

// PutTiddler upserts a tiddler in SQLite and writes its human-readable JSON backup.
func (s *TiddlerStore) PutTiddler(title string, payload map[string]any) (int, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	modified := stringOrDefault(payload["modified"], currentTwTimestamp())
	contentType := stringOrDefault(payload["type"], "text/vnd.tiddlywiki")

	var textVal any
	if rawText, ok := payload["text"].(string); ok {
		textVal = rawText
	}

	extraFields := make(map[string]any)
	for k, v := range payload {
		if !coreTiddlerKeys[k] {
			extraFields[k] = v
		}
	}

	var blobHash any
	if canonicalURI, ok := extraFields["_canonical_uri"].(string); ok && strings.HasPrefix(canonicalURI, "files/") {
		blobHash = strings.TrimPrefix(canonicalURI, "files/")
	} else if bh, ok := payload["blob_hash"].(string); ok && bh != "" {
		blobHash = bh
	}

	fieldsBytes, err := json.Marshal(extraFields)
	if err != nil {
		return 0, err
	}

	var nextRev int
	if err := s.db.QueryRow(`SELECT COALESCE(MAX(revision), 0) + 1 FROM tiddlers`).Scan(&nextRev); err != nil {
		return 0, err
	}

	_, err = s.db.Exec(`
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
	`, title, nextRev, modified, contentType, string(fieldsBytes), textVal, blobHash)
	if err != nil {
		return 0, err
	}

	fullTiddler := make(map[string]any, len(extraFields)+6)
	for k, v := range extraFields {
		fullTiddler[k] = v
	}
	fullTiddler["title"] = title
	fullTiddler["revision"] = nextRev
	fullTiddler["modified"] = modified
	fullTiddler["type"] = contentType
	fullTiddler["bag"] = "default"
	if textVal != nil {
		fullTiddler["text"] = textVal
	}

	if err := s.WriteTiddlerBackup(title, fullTiddler, false); err != nil {
		return 0, err
	}
	return nextRev, nil
}

// DeleteTiddler soft-deletes a tiddler via tombstone revision and updates its backup.
func (s *TiddlerStore) DeleteTiddler(title string) (bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	var deleted int
	err := s.db.QueryRow(`SELECT deleted FROM tiddlers WHERE title = ?`, title).Scan(&deleted)
	if errors.Is(err, sql.ErrNoRows) || deleted == 1 {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	nowTw := currentTwTimestamp()
	var nextRev int
	if err := s.db.QueryRow(`SELECT COALESCE(MAX(revision), 0) + 1 FROM tiddlers`).Scan(&nextRev); err != nil {
		return false, err
	}

	if _, err := s.db.Exec(`
		UPDATE tiddlers
		SET revision = ?, modified = ?, text = NULL, deleted = 1, dirty = 0
		WHERE title = ?
	`, nextRev, nowTw, title); err != nil {
		return false, err
	}

	tombstone := map[string]any{
		"title":    title,
		"revision": nextRev,
		"modified": nowTw,
		"deleted":  true,
	}
	if err := s.WriteTiddlerBackup(title, tombstone, true); err != nil {
		return false, err
	}
	return true, nil
}

// GetBlob retrieves a binary blob and its MIME type by SHA-256 hash.
func (s *TiddlerStore) GetBlob(sha256Hash string) (string, []byte, error) {
	var mimeType string
	var data []byte
	err := s.db.QueryRow(`SELECT mime_type, data FROM blobs WHERE sha256 = ?`, sha256Hash).Scan(&mimeType, &data)
	if err == nil {
		return mimeType, data, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", nil, err
	}

	backupPath := filepath.Join(s.blobBackupDir, sha256Hash)
	raw, readErr := os.ReadFile(backupPath)
	if os.IsNotExist(readErr) {
		return "", nil, nil
	}
	if readErr != nil {
		return "", nil, readErr
	}
	return "application/octet-stream", raw, nil
}

// PutBlob verifies SHA-256 and stores a binary blob in SQLite and backup disk.
func (s *TiddlerStore) PutBlob(expectedSha256, mimeType string, data []byte) (string, error) {
	sum := sha256.Sum256(data)
	actualSha256 := hex.EncodeToString(sum[:])
	if expectedSha256 != "" && expectedSha256 != actualSha256 {
		return "", fmt.Errorf("SHA-256 mismatch: expected %s, got %s", expectedSha256, actualSha256)
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	_, err := s.db.Exec(`
		INSERT INTO blobs (sha256, mime_type, size_bytes, data)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(sha256) DO NOTHING
	`, actualSha256, mimeType, len(data), data)
	if err != nil {
		return "", err
	}

	if err := s.WriteBlobBackup(actualSha256, data); err != nil {
		return "", err
	}
	return actualSha256, nil
}

// SyncBatch applies client delta changes and returns server changes since `SinceRevision`.
func (s *TiddlerStore) SyncBatch(req SyncRequest) (SyncResponse, error) {
	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}

	for _, tiddler := range req.Upserted {
		if title, ok := tiddler["title"].(string); ok && title != "" {
			if _, err := s.PutTiddler(title, tiddler); err != nil {
				return SyncResponse{}, err
			}
		}
	}
	for _, deletedTitle := range req.Deleted {
		if _, err := s.DeleteTiddler(deletedTitle); err != nil {
			return SyncResponse{}, err
		}
	}

	rows, err := s.db.Query(`
		SELECT title, revision, modified, type, fields_json, text, COALESCE(blob_hash, ''), deleted
		FROM tiddlers
		WHERE revision > ?
		ORDER BY revision ASC
		LIMIT ?
	`, req.SinceRevision, limit)
	if err != nil {
		return SyncResponse{}, err
	}
	defer rows.Close()

	changed := make([]map[string]any, 0)
	deleted := make([]string, 0)
	for rows.Next() {
		var title, modified, contentType, fieldsJSON, blobHash string
		var revision, isDeleted int
		var text sql.NullString
		if err := rows.Scan(&title, &revision, &modified, &contentType, &fieldsJSON, &text, &blobHash, &isDeleted); err != nil {
			return SyncResponse{}, err
		}
		if isDeleted == 1 {
			deleted = append(deleted, title)
		} else {
			var textPtr *string
			if text.Valid {
				textPtr = &text.String
			}
			changed = append(changed, rowToTiddler(title, revision, modified, contentType, fieldsJSON, textPtr, blobHash, true))
		}
	}

	var currentRev int
	if err := s.db.QueryRow(`SELECT COALESCE(MAX(revision), 0) FROM tiddlers`).Scan(&currentRev); err != nil {
		return SyncResponse{}, err
	}

	return SyncResponse{
		CurrentRevision: currentRev,
		Changed:         changed,
		Deleted:         deleted,
	}, nil
}

// WriteTiddlerBackup writes an atomic human-readable JSON snapshot of a tiddler.
func (s *TiddlerStore) WriteTiddlerBackup(title string, tiddlerObj map[string]any, deleted bool) error {
	safeName := url.PathEscape(title)
	activePath := filepath.Join(s.tiddlerBackupDir, safeName+".json")
	deletedPath := filepath.Join(s.tiddlerBackupDir, safeName+".deleted.json")

	targetPath := activePath
	stalePath := deletedPath
	if deleted {
		targetPath = deletedPath
		stalePath = activePath
	}
	_ = os.Remove(stalePath)

	encoded, err := json.MarshalIndent(tiddlerObj, "", "  ")
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')

	tempPath := targetPath + ".tmp"
	if err := os.WriteFile(tempPath, encoded, 0o644); err != nil {
		return err
	}
	return os.Rename(tempPath, targetPath)
}

// WriteBlobBackup writes an atomic raw binary file to backup/blobs/<sha256>.
func (s *TiddlerStore) WriteBlobBackup(sha256Hash string, data []byte) error {
	targetPath := filepath.Join(s.blobBackupDir, sha256Hash)
	if _, err := os.Stat(targetPath); err == nil {
		return nil
	}
	tempPath := targetPath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tempPath, targetPath)
}

func rowToTiddler(title string, revision int, modified, contentType, fieldsJSON string, text *string, blobHash string, includeText bool) map[string]any {
	fields := make(map[string]any)
	_ = json.Unmarshal([]byte(fieldsJSON), &fields)
	fields["title"] = title
	fields["revision"] = revision
	fields["modified"] = modified
	fields["type"] = contentType
	fields["bag"] = "default"
	if blobHash != "" {
		if _, exists := fields["_canonical_uri"]; !exists {
			fields["_canonical_uri"] = "files/" + blobHash
		}
	}
	if includeText && text != nil {
		fields["text"] = *text
	}
	return fields
}

func stringOrDefault(val any, fallback string) string {
	if s, ok := val.(string); ok && s != "" {
		return s
	}
	return fallback
}

func currentTwTimestamp() string {
	now := time.Now().UTC()
	return now.Format("20060102150405") + fmt.Sprintf("%03d", now.Nanosecond()/1e6)
}

func readBody(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			return nil, err
		}
		defer gz.Close()
		return io.ReadAll(gz)
	}
	return io.ReadAll(r.Body)
}

func sendJSON(w http.ResponseWriter, r *http.Request, status int, payload any) {
	raw, err := json.Marshal(payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") && len(raw) > 512 {
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(status)
		gz := gzip.NewWriter(w)
		_, _ = gz.Write(raw)
		_ = gz.Close()
		return
	}
	w.WriteHeader(status)
	_, _ = w.Write(raw)
}

func main() {
	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = filepath.Join("..", "..", "data")
	}
	schemaPath := filepath.Join("..", "schema.sql")

	store, err := NewTiddlerStore(dataDir, schemaPath)
	if err != nil {
		log.Fatalf("failed to initialize storage: %v", err)
	}

	mux := http.NewServeMux()

	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		sendJSON(w, r, http.StatusOK, store.GetStatus())
	})

	mux.HandleFunc("GET /recipes/default/tiddlers.json", func(w http.ResponseWriter, r *http.Request) {
		list, err := store.ListSkinnyTiddlers()
		if err != nil {
			sendJSON(w, r, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		sendJSON(w, r, http.StatusOK, list)
	})

	mux.HandleFunc("GET /recipes/default/tiddlers/{title}", func(w http.ResponseWriter, r *http.Request) {
		title, _ := url.PathUnescape(r.PathValue("title"))
		tiddler, err := store.GetTiddler(title)
		if err != nil {
			sendJSON(w, r, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if tiddler == nil {
			sendJSON(w, r, http.StatusNotFound, map[string]string{"error": "Not found"})
			return
		}
		sendJSON(w, r, http.StatusOK, tiddler)
	})

	mux.HandleFunc("PUT /recipes/default/tiddlers/{title}", func(w http.ResponseWriter, r *http.Request) {
		title, _ := url.PathUnescape(r.PathValue("title"))
		body, err := readBody(r)
		if err != nil {
			sendJSON(w, r, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			sendJSON(w, r, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		rev, err := store.PutTiddler(title, payload)
		if err != nil {
			sendJSON(w, r, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		w.Header().Set("ETag", fmt.Sprintf(`"default/%s/%d:"`, url.PathEscape(title), rev))
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("DELETE /bags/default/tiddlers/{title}", func(w http.ResponseWriter, r *http.Request) {
		title, _ := url.PathUnescape(r.PathValue("title"))
		deleted, err := store.DeleteTiddler(title)
		if err != nil {
			sendJSON(w, r, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if !deleted {
			sendJSON(w, r, http.StatusNotFound, map[string]string{"error": "Not found"})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("GET /files/{sha256}", func(w http.ResponseWriter, r *http.Request) {
		sha256Hash := r.PathValue("sha256")
		mimeType, data, err := store.GetBlob(sha256Hash)
		if err != nil {
			sendJSON(w, r, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if data == nil {
			sendJSON(w, r, http.StatusNotFound, map[string]string{"error": "Blob not found"})
			return
		}
		w.Header().Set("Content-Type", mimeType)
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	})

	mux.HandleFunc("PUT /files/{sha256}", func(w http.ResponseWriter, r *http.Request) {
		sha256Hash := r.PathValue("sha256")
		body, err := readBody(r)
		if err != nil {
			sendJSON(w, r, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		mimeType := r.Header.Get("Content-Type")
		if mimeType == "" {
			mimeType = "application/octet-stream"
		}
		storedHash, err := store.PutBlob(sha256Hash, mimeType, body)
		if err != nil {
			sendJSON(w, r, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		sendJSON(w, r, http.StatusCreated, map[string]string{
			"sha256": storedHash,
			"uri":    "files/" + storedHash,
		})
	})

	mux.HandleFunc("POST /api/sync", func(w http.ResponseWriter, r *http.Request) {
		body, err := readBody(r)
		if err != nil {
			sendJSON(w, r, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		var syncReq SyncRequest
		if err := json.Unmarshal(body, &syncReq); err != nil {
			sendJSON(w, r, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		resp, err := store.SyncBatch(syncReq)
		if err != nil {
			sendJSON(w, r, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		sendJSON(w, r, http.StatusOK, resp)
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("Go TiddlyWiki server listening on http://localhost:%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
