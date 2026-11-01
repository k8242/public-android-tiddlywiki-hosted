package com.example.tiddlywiki.sync

import android.content.Context
import androidx.work.Constraints
import androidx.work.CoroutineWorker
import androidx.work.ExistingPeriodicWorkPolicy
import androidx.work.NetworkType
import androidx.work.PeriodicWorkRequestBuilder
import androidx.work.WorkManager
import androidx.work.WorkerParameters
import com.example.tiddlywiki.data.TiddlerStorage
import java.io.IOException
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONArray
import org.json.JSONObject

/**
 * Periodic background worker (`WorkManager`) that synchronizes locally modified tiddlers (`dirty = 1`)
 * and content-addressed binary blobs (`>100 KB`) with any of the four backend servers
 * (Node.js, Java/Javalin, Go, or Python).
 */
class TiddlerSyncWorker(
    appContext: Context,
    workerParams: WorkerParameters
) : CoroutineWorker(appContext, workerParams) {

    private val storage = TiddlerStorage(appContext)
    private val prefs = appContext.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
    private val httpClient = OkHttpClient()

    override suspend fun doWork(): Result = withContext(Dispatchers.IO) {
        val baseUrl = prefs.getString(KEY_SERVER_URL, DEFAULT_SERVER_URL)!!.trimEnd('/')
        val lastSyncRevision = prefs.getInt(KEY_LAST_SYNC_REV, 0)

        try {
            val deltaPayload = storage.collectDirtyDelta(lastSyncRevision)

            // 1. Upload any referenced binary blobs before pushing tiddler metadata
            val upserted = deltaPayload.optJSONArray("upserted") ?: JSONArray()
            uploadDirtyBlobs(baseUrl, upserted)

            // 2. Exchange tiddler delta in a single batch HTTP roundtrip (POST /api/sync)
            val syncRequest = Request.Builder()
                .url("$baseUrl/api/sync")
                .post(deltaPayload.toString().toRequestBody(JSON_MEDIA_TYPE))
                .build()

            val serverResponse = httpClient.newCall(syncRequest).execute().use { response ->
                if (!response.isSuccessful) {
                    throw IOException("Sync failed with HTTP ${response.code}")
                }
                JSONObject(response.body?.string() ?: "{}")
            }

            // 3. Download any missing binary blobs referenced by incoming server tiddlers
            val changed = serverResponse.optJSONArray("changed") ?: JSONArray()
            downloadMissingBlobs(baseUrl, changed)

            // 4. Apply server delta to local SQLite + backup mirror and persist new revision
            val newServerRevision = storage.syncBatch(serverResponse)
            prefs.edit().putInt(KEY_LAST_SYNC_REV, newServerRevision).apply()

            Result.success()
        } catch (_: IOException) {
            Result.retry()
        }
    }

    private fun uploadDirtyBlobs(baseUrl: String, upsertedTiddlers: JSONArray) {
        for (i in 0 until upsertedTiddlers.length()) {
            val tiddler = upsertedTiddlers.getJSONObject(i)
            val canonicalUri = tiddler.optString("_canonical_uri", "")
            if (canonicalUri.startsWith(CANONICAL_URI_PREFIX)) {
                val sha256Hash = canonicalUri.removePrefix(CANONICAL_URI_PREFIX)
                val blob = storage.getBlob(sha256Hash) ?: continue
                val putRequest = Request.Builder()
                    .url("$baseUrl/files/$sha256Hash")
                    .put(blob.data.toRequestBody(blob.mimeType.toMediaType()))
                    .build()
                httpClient.newCall(putRequest).execute().use { resp ->
                    if (!resp.isSuccessful) {
                        throw IOException("Blob upload failed for $sha256Hash: HTTP ${resp.code}")
                    }
                }
            }
        }
    }

    private fun downloadMissingBlobs(baseUrl: String, changedTiddlers: JSONArray) {
        for (i in 0 until changedTiddlers.length()) {
            val tiddler = changedTiddlers.getJSONObject(i)
            val canonicalUri = tiddler.optString("_canonical_uri", "")
            if (canonicalUri.startsWith(CANONICAL_URI_PREFIX)) {
                val sha256Hash = canonicalUri.removePrefix(CANONICAL_URI_PREFIX)
                if (storage.getBlob(sha256Hash) != null) continue

                val getRequest = Request.Builder()
                    .url("$baseUrl/files/$sha256Hash")
                    .get()
                    .build()
                httpClient.newCall(getRequest).execute().use { resp ->
                    if (resp.isSuccessful) {
                        val mimeType = resp.header("Content-Type") ?: "application/octet-stream"
                        val bytes = resp.body?.bytes() ?: return@use
                        storage.putBlob(sha256Hash, mimeType, bytes)
                    }
                }
            }
        }
    }

    companion object {
        private const val WORK_NAME = "TiddlyWikiPeriodicSync"
        private const val PREFS_NAME = "tiddlywiki_sync_prefs"
        private const val KEY_SERVER_URL = "server_url"
        private const val KEY_LAST_SYNC_REV = "last_sync_revision"
        private const val DEFAULT_SERVER_URL = "http://10.0.2.2:8080"
        private const val CANONICAL_URI_PREFIX = "files/"
        private val JSON_MEDIA_TYPE = "application/json; charset=utf-8".toMediaType()

        /** Enqueue 15-minute periodic background sync when network connectivity is available. */
        fun schedulePeriodicSync(context: Context) {
            val constraints = Constraints.Builder()
                .setRequiredNetworkType(NetworkType.CONNECTED)
                .build()

            val syncRequest = PeriodicWorkRequestBuilder<TiddlerSyncWorker>(
                15,
                TimeUnit.MINUTES
            )
                .setConstraints(constraints)
                .build()

            WorkManager.getInstance(context).enqueueUniquePeriodicWork(
                WORK_NAME,
                ExistingPeriodicWorkPolicy.KEEP,
                syncRequest
            )
        }
    }
}
