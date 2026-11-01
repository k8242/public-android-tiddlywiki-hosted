package com.example.tiddlywiki.webview

import android.net.Uri
import android.webkit.JavascriptInterface
import android.webkit.WebResourceRequest
import android.webkit.WebResourceResponse
import android.webkit.WebView
import android.webkit.WebViewClient
import com.example.tiddlywiki.data.TiddlerStorage
import java.io.ByteArrayInputStream
import java.net.URLDecoder
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.Base64
import org.json.JSONObject

/**
 * Intercepts TiddlyWiki `tiddlyweb` and `/files/{sha256}` requests inside the Android `WebView`
 * and routes them directly to local SQLite (`TiddlerStorage`) without running a local TCP socket.
 *
 * - Read requests (`GET /status`, `GET /recipes/default/tiddlers.json`,
 *   `GET /recipes/default/tiddlers/{title}`, `GET /files/{sha256}`) stream via
 *   `shouldInterceptRequest()` as `ByteArrayInputStream`s (bypassing JNI string limits).
 * - Write requests (`PUT` / `DELETE`) are routed through `AndroidTiddlyBridge` to persist
 *   to local SQLite + JSON/binary backup files in <2ms while marking `dirty = 1` for WorkManager.
 */
class TiddlerWebViewInterceptor(
    private val storage: TiddlerStorage,
    private val assetProvider: (String) -> ByteArray?
) : WebViewClient() {

    override fun shouldInterceptRequest(
        view: WebView,
        request: WebResourceRequest
    ): WebResourceResponse? {
        val uri: Uri = request.url
        if (uri.host != VIRTUAL_HOST) {
            return super.shouldInterceptRequest(view, request)
        }

        val path = uri.path ?: "/"
        if (request.method.equals("GET", ignoreCase = true)) {
            return handleGetRequest(path)
        }
        return jsonResponse(405, """{"error":"Use AndroidTiddlyBridge for write operations"}""")
    }

    private fun handleGetRequest(path: String): WebResourceResponse {
        if (path == "/status") {
            return jsonResponse(200, storage.getStatus().toString())
        }

        if (path == "/recipes/default/tiddlers.json") {
            return jsonResponse(200, storage.listSkinnyTiddlers().toString())
        }

        if (path.startsWith(TIDDLER_PATH_PREFIX)) {
            val encodedTitle = path.removePrefix(TIDDLER_PATH_PREFIX)
            val title = URLDecoder.decode(encodedTitle, StandardCharsets.UTF_8.name())
            val tiddler = storage.getTiddler(title)
                ?: return jsonResponse(404, """{"error":"Not found"}""")
            return jsonResponse(200, tiddler.toString())
        }

        if (path.startsWith(FILES_PATH_PREFIX)) {
            val sha256Hash = path.removePrefix(FILES_PATH_PREFIX)
            val blob = storage.getBlob(sha256Hash)
                ?: return jsonResponse(404, """{"error":"Blob not found"}""")
            return WebResourceResponse(
                blob.mimeType,
                null,
                200,
                "OK",
                mapOf("Content-Length" to blob.data.size.toString()),
                ByteArrayInputStream(blob.data)
            )
        }

        if (path.startsWith(ASSETS_PATH_PREFIX)) {
            val assetName = path.removePrefix(ASSETS_PATH_PREFIX)
            val bytes = assetProvider(assetName)
                ?: return jsonResponse(404, """{"error":"Asset not found"}""")
            return WebResourceResponse(
                "text/html",
                "UTF-8",
                200,
                "OK",
                emptyMap(),
                ByteArrayInputStream(bytes)
            )
        }

        return jsonResponse(404, """{"error":"Route not found"}""")
    }

    private fun jsonResponse(statusCode: Int, jsonBody: String): WebResourceResponse {
        val bytes = jsonBody.toByteArray(StandardCharsets.UTF_8)
        val reason = if (statusCode in 200..299) "OK" else "Error"
        return WebResourceResponse(
            "application/json",
            "UTF-8",
            statusCode,
            reason,
            mapOf(
                "Access-Control-Allow-Origin" to "*",
                "Content-Length" to bytes.size.toString()
            ),
            ByteArrayInputStream(bytes)
        )
    }

    /**
     * JavaScript bridge exposed as `window.AndroidTiddlyBridge` inside the WebView for
     * synchronous/atomic local SQLite writes when TiddlyWiki's syncadaptor saves or deletes.
     */
    class JsBridge(private val storage: TiddlerStorage) {

        @JavascriptInterface
        fun putTiddler(title: String, tiddlerJson: String): String {
            val payload = JSONObject(tiddlerJson)
            val revision = storage.putTiddler(title, payload, markDirty = true)
            val encodedTitle = URLEncoder.encode(title, StandardCharsets.UTF_8.name())
            return "\"default/$encodedTitle/$revision:\""
        }

        @JavascriptInterface
        fun deleteTiddler(title: String): Boolean =
            storage.deleteTiddler(title, markDirty = true)

        @JavascriptInterface
        fun putBlobBase64(expectedSha256: String, mimeType: String, base64Data: String): String {
            val rawBytes = Base64.getDecoder().decode(base64Data)
            return storage.putBlob(expectedSha256, mimeType, rawBytes)
        }
    }

    companion object {
        const val VIRTUAL_HOST = "appassets.androidplatform.net"
        const val BOOT_URL = "https://$VIRTUAL_HOST/assets/tiddlywiki_boot.html"
        const val JS_BRIDGE_NAME = "AndroidTiddlyBridge"
        private const val TIDDLER_PATH_PREFIX = "/recipes/default/tiddlers/"
        private const val FILES_PATH_PREFIX = "/files/"
        private const val ASSETS_PATH_PREFIX = "/assets/"
    }
}
