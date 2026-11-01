package com.example.tiddlywiki.data

import android.content.ContentValues
import android.content.Context
import android.database.sqlite.SQLiteDatabase
import android.database.sqlite.SQLiteOpenHelper
import java.io.File
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import org.json.JSONArray
import org.json.JSONObject

/**
 * Dual SQLite + filesystem backup storage for the standalone Android TiddlyWiki app.
 *
 * Uses the exact same table layout (`tiddlers` and `blobs`) and method naming conventions
 * (`initStorage`, `getStatus`, `listSkinnyTiddlers`, `getTiddler`, `putTiddler`,
 * `deleteTiddler`, `getBlob`, `putBlob`, `syncBatch`, `writeTiddlerBackup`, `writeBlobBackup`)
 * as the JS, Java, Go, and Python backend servers.
 */
class TiddlerStorage(context: Context) :
    SQLiteOpenHelper(
        context,
        File(File(context.filesDir, "data").apply { mkdirs() }, "tiddlywiki.db").absolutePath,
        null,
        DATABASE_VERSION
    ) {

    private val dataDir = File(context.filesDir, "data")
    private val tiddlerBackupDir = File(dataDir, "backup/tiddlers")
    private val blobBackupDir = File(dataDir, "backup/blobs")

    init {
        initStorage()
    }

    override fun onConfigure(db: SQLiteDatabase) {
        super.onConfigure(db)
        db.enableWriteAheadLogging()
    }

    override fun onCreate(db: SQLiteDatabase) {
        db.execSQL(
            """
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
            )
            """.trimIndent()
        )
        db.execSQL("CREATE INDEX IF NOT EXISTS idx_tiddlers_revision ON tiddlers(revision)")
        db.execSQL(
            "CREATE INDEX IF NOT EXISTS idx_tiddlers_dirty ON tiddlers(dirty) WHERE dirty = 1"
        )
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS blobs (
                sha256 TEXT PRIMARY KEY,
                mime_type TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                data BLOB NOT NULL
            )
            """.trimIndent()
        )
    }

    override fun onUpgrade(db: SQLiteDatabase, oldVersion: Int, newVersion: Int) {
        onCreate(db)
    }

    /** Initialize local SQLite database and backup directories under `filesDir/data/`. */
    fun initStorage() {
        tiddlerBackupDir.mkdirs()
        blobBackupDir.mkdirs()
        writableDatabase // Triggers onCreate if not yet initialized
    }

    /** Return tiddlyweb server status metadata. */
    fun getStatus(): JSONObject =
        JSONObject()
            .put("username", "android-user")
            .put("space", JSONObject().put("recipe", DEFAULT_RECIPE))
            .put("tiddlywiki_version", TIDDLYWIKI_VERSION)

    /** Return all non-deleted tiddlers omitting the heavy `text` field. */
    fun listSkinnyTiddlers(): JSONArray {
        val array = JSONArray()
        readableDatabase.rawQuery(
            """
            SELECT title, revision, modified, type, fields_json, blob_hash
            FROM tiddlers
            WHERE deleted = 0
            ORDER BY modified DESC
            """.trimIndent(),
            null
        ).use { cursor ->
            while (cursor.moveToNext()) {
                array.put(
                    rowToTiddler(
                        title = cursor.getString(0),
                        revision = cursor.getInt(1),
                        modified = cursor.getString(2),
                        type = cursor.getString(3),
                        fieldsJson = cursor.getString(4),
                        text = null,
                        blobHash = cursor.optStringOrNull(5),
                        includeText = false
                    )
                )
            }
        }
        return array
    }

    /** Fetch a single full tiddler (including `text`) by title. */
    fun getTiddler(title: String): JSONObject? {
        readableDatabase.rawQuery(
            """
            SELECT title, revision, modified, type, fields_json, text, blob_hash
            FROM tiddlers
            WHERE title = ? AND deleted = 0
            """.trimIndent(),
            arrayOf(title)
        ).use { cursor ->
            if (!cursor.moveToFirst()) return null
            return rowToTiddler(
                title = cursor.getString(0),
                revision = cursor.getInt(1),
                modified = cursor.getString(2),
                type = cursor.getString(3),
                fieldsJson = cursor.getString(4),
                text = cursor.optStringOrNull(5),
                blobHash = cursor.optStringOrNull(6),
                includeText = true
            )
        }
    }

    /**
     * Upsert a tiddler in local SQLite and write its human-readable JSON backup.
     *
     * @param markDirty `true` when edited locally inside the Android WebView; `false` when
     *   applying clean changes pulled from the remote server during `WorkManager` sync.
     */
    @Synchronized
    fun putTiddler(title: String, payload: JSONObject, markDirty: Boolean = true): Int {
        val modified = payload.optString("modified").ifEmpty { currentTwTimestamp() }
        val type = payload.optString("type").ifEmpty { DEFAULT_TIDDLER_TYPE }
        val text = if (payload.has("text") && !payload.isNull("text")) {
            payload.getString("text")
        } else {
            null
        }

        val extraFields = JSONObject()
        val keys = payload.keys()
        while (keys.hasNext()) {
            val key = keys.next()
            if (key !in CORE_TIDDLER_KEYS) {
                extraFields.put(key, payload.get(key))
            }
        }

        val canonicalUri = extraFields.optString("_canonical_uri", "")
        val blobHash = when {
            canonicalUri.startsWith(CANONICAL_URI_PREFIX) ->
                canonicalUri.removePrefix(CANONICAL_URI_PREFIX)
            payload.has("blob_hash") && !payload.isNull("blob_hash") ->
                payload.getString("blob_hash")
            else -> null
        }

        val db = writableDatabase
        val nextRev = if (!markDirty && payload.has("revision")) {
            payload.getInt("revision")
        } else {
            nextRevision(db)
        }

        val values = ContentValues().apply {
            put("title", title)
            put("revision", nextRev)
            put("modified", modified)
            put("type", type)
            put("fields_json", extraFields.toString())
            if (text != null) put("text", text) else putNull("text")
            if (blobHash != null) put("blob_hash", blobHash) else putNull("blob_hash")
            put("deleted", 0)
            put("dirty", if (markDirty) 1 else 0)
        }
        db.insertWithOnConflict("tiddlers", null, values, SQLiteDatabase.CONFLICT_REPLACE)

        val fullTiddler = rowToTiddler(
            title = title,
            revision = nextRev,
            modified = modified,
            type = type,
            fieldsJson = extraFields.toString(),
            text = text,
            blobHash = blobHash,
            includeText = true
        )
        writeTiddlerBackup(title, fullTiddler, deleted = false)
        return nextRev
    }

    /** Soft-delete a tiddler via tombstone revision and update its backup. */
    @Synchronized
    fun deleteTiddler(title: String, markDirty: Boolean = true): Boolean {
        val db = writableDatabase
        val exists = db.rawQuery(
            "SELECT deleted FROM tiddlers WHERE title = ?",
            arrayOf(title)
        ).use { cursor ->
            cursor.moveToFirst() && cursor.getInt(0) == 0
        }
        if (!exists && markDirty) return false

        val nowTw = currentTwTimestamp()
        val nextRev = nextRevision(db)
        val values = ContentValues().apply {
            put("revision", nextRev)
            put("modified", nowTw)
            putNull("text")
            put("deleted", 1)
            put("dirty", if (markDirty) 1 else 0)
        }
        db.update("tiddlers", values, "title = ?", arrayOf(title))

        val tombstone = JSONObject()
            .put("title", title)
            .put("revision", nextRev)
            .put("modified", nowTw)
            .put("deleted", true)
        writeTiddlerBackup(title, tombstone, deleted = true)
        return true
    }

    /** Retrieve a binary blob and its MIME type by SHA-256 hash. */
    fun getBlob(sha256Hash: String): BlobRecord? {
        readableDatabase.rawQuery(
            "SELECT mime_type, data FROM blobs WHERE sha256 = ?",
            arrayOf(sha256Hash)
        ).use { cursor ->
            if (cursor.moveToFirst()) {
                return BlobRecord(cursor.getString(0), cursor.getBlob(1))
            }
        }
        val backupFile = File(blobBackupDir, sha256Hash)
        if (backupFile.isFile) {
            return BlobRecord("application/octet-stream", backupFile.readBytes())
        }
        return null
    }

    /** Verify SHA-256 and store a binary blob in local SQLite and backup disk. */
    @Synchronized
    fun putBlob(expectedSha256: String, mimeType: String, data: ByteArray): String {
        val actualSha256 = sha256Hex(data)
        require(expectedSha256.isEmpty() || expectedSha256 == actualSha256) {
            "SHA-256 mismatch: expected $expectedSha256, got $actualSha256"
        }

        val values = ContentValues().apply {
            put("sha256", actualSha256)
            put("mime_type", mimeType)
            put("size_bytes", data.size)
            put("data", data)
        }
        writableDatabase.insertWithOnConflict(
            "blobs",
            null,
            values,
            SQLiteDatabase.CONFLICT_IGNORE
        )
        writeBlobBackup(actualSha256, data)
        return actualSha256
    }

    /** Collect locally modified (`dirty = 1`) tiddlers and tombstones for WorkManager sync. */
    fun collectDirtyDelta(sinceServerRevision: Int): JSONObject {
        val upserted = JSONArray()
        val deleted = JSONArray()

        readableDatabase.rawQuery(
            """
            SELECT title, revision, modified, type, fields_json, text, blob_hash, deleted
            FROM tiddlers
            WHERE dirty = 1
            ORDER BY revision ASC
            """.trimIndent(),
            null
        ).use { cursor ->
            while (cursor.moveToNext()) {
                val title = cursor.getString(0)
                val isDeleted = cursor.getInt(7) == 1
                if (isDeleted) {
                    deleted.put(title)
                } else {
                    upserted.put(
                        rowToTiddler(
                            title = title,
                            revision = cursor.getInt(1),
                            modified = cursor.getString(2),
                            type = cursor.getString(3),
                            fieldsJson = cursor.getString(4),
                            text = cursor.optStringOrNull(5),
                            blobHash = cursor.optStringOrNull(6),
                            includeText = true
                        )
                    )
                }
            }
        }

        return JSONObject()
            .put("sinceRevision", sinceServerRevision)
            .put("limit", DEFAULT_BATCH_LIMIT)
            .put("upserted", upserted)
            .put("deleted", deleted)
    }

    /** Apply the remote server's `/api/sync` delta response and clear local `dirty` flags. */
    @Synchronized
    fun syncBatch(serverResponse: JSONObject): Int {
        val db = writableDatabase
        db.execSQL("UPDATE tiddlers SET dirty = 0 WHERE dirty = 1")

        val changed = serverResponse.optJSONArray("changed") ?: JSONArray()
        for (i in 0 until changed.length()) {
            val tiddler = changed.getJSONObject(i)
            val title = tiddler.optString("title")
            if (title.isNotEmpty()) {
                putTiddler(title, tiddler, markDirty = false)
            }
        }

        val deleted = serverResponse.optJSONArray("deleted") ?: JSONArray()
        for (i in 0 until deleted.length()) {
            val title = deleted.getString(i)
            deleteTiddler(title, markDirty = false)
        }

        return serverResponse.optInt("currentRevision", 0)
    }

    /** Write an atomic human-readable JSON snapshot of a tiddler to `backup/tiddlers/`. */
    fun writeTiddlerBackup(title: String, tiddlerObj: JSONObject, deleted: Boolean): File {
        val safeName = URLEncoder.encode(title, StandardCharsets.UTF_8.name())
        val activeFile = File(tiddlerBackupDir, "$safeName.json")
        val deletedFile = File(tiddlerBackupDir, "$safeName.deleted.json")

        val targetFile = if (deleted) deletedFile else activeFile
        val staleFile = if (deleted) activeFile else deletedFile
        if (staleFile.exists()) {
            staleFile.delete()
        }

        val tempFile = File(tiddlerBackupDir, "${targetFile.name}.tmp")
        tempFile.writeText(tiddlerObj.toString(2) + "\n", Charsets.UTF_8)
        tempFile.renameTo(targetFile)
        return targetFile
    }

    /** Write an atomic raw binary file to `backup/blobs/<sha256>`. */
    fun writeBlobBackup(sha256Hash: String, data: ByteArray): File {
        val targetFile = File(blobBackupDir, sha256Hash)
        if (!targetFile.exists()) {
            val tempFile = File(blobBackupDir, "$sha256Hash.tmp")
            tempFile.writeBytes(data)
            tempFile.renameTo(targetFile)
        }
        return targetFile
    }

    private fun nextRevision(db: SQLiteDatabase): Int {
        db.rawQuery("SELECT COALESCE(MAX(revision), 0) + 1 FROM tiddlers", null).use { cursor ->
            cursor.moveToFirst()
            return cursor.getInt(0)
        }
    }

    private fun rowToTiddler(
        title: String,
        revision: Int,
        modified: String,
        type: String,
        fieldsJson: String?,
        text: String?,
        blobHash: String?,
        includeText: Boolean
    ): JSONObject {
        val result = if (!fieldsJson.isNullOrEmpty()) JSONObject(fieldsJson) else JSONObject()
        result.put("title", title)
        result.put("revision", revision)
        result.put("modified", modified)
        result.put("type", type)
        result.put("bag", DEFAULT_BAG)
        if (!blobHash.isNullOrEmpty() && !result.has("_canonical_uri")) {
            result.put("_canonical_uri", "$CANONICAL_URI_PREFIX$blobHash")
        }
        if (includeText && text != null) {
            result.put("text", text)
        }
        return result
    }

    data class BlobRecord(val mimeType: String, val data: ByteArray) {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is BlobRecord) return false
            return mimeType == other.mimeType && data.contentEquals(other.data)
        }

        override fun hashCode(): Int = 31 * mimeType.hashCode() + data.contentHashCode()
    }

    companion object {
        private const val DATABASE_VERSION = 1
        private const val DEFAULT_BATCH_LIMIT = 100
        private const val DEFAULT_TIDDLER_TYPE = "text/vnd.tiddlywiki"
        private const val DEFAULT_BAG = "default"
        private const val DEFAULT_RECIPE = "default"
        private const val TIDDLYWIKI_VERSION = "5.3.5"
        private const val CANONICAL_URI_PREFIX = "files/"

        private val CORE_TIDDLER_KEYS = setOf(
            "title",
            "revision",
            "modified",
            "type",
            "text",
            "bag"
        )

        private val TW_TIMESTAMP_FMT: DateTimeFormatter =
            DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS").withZone(ZoneOffset.UTC)

        private fun currentTwTimestamp(): String = TW_TIMESTAMP_FMT.format(Instant.now())

        private fun sha256Hex(data: ByteArray): String =
            MessageDigest.getInstance("SHA-256")
                .digest(data)
                .joinToString("") { "%02x".format(it) }

        private fun android.database.Cursor.optStringOrNull(index: Int): String? =
            if (isNull(index)) null else getString(index)
    }
}
