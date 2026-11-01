package com.example.tiddlywiki

import android.annotation.SuppressLint
import android.os.Bundle
import android.webkit.WebView
import androidx.appcompat.app.AppCompatActivity
import com.example.tiddlywiki.data.TiddlerStorage
import com.example.tiddlywiki.sync.TiddlerSyncWorker
import com.example.tiddlywiki.webview.TiddlerWebViewInterceptor

/**
 * Hosts the standalone TiddlyWiki `WebView` backed by local SQLite (`TiddlerStorage`)
 * and schedules 15-minute periodic delta synchronization (`TiddlerSyncWorker`).
 */
class MainActivity : AppCompatActivity() {

    private lateinit var storage: TiddlerStorage
    private lateinit var webView: WebView

    @SuppressLint("SetJavaScriptEnabled")
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        storage = TiddlerStorage(applicationContext)
        webView = WebView(this).apply {
            settings.javaScriptEnabled = true
            settings.domStorageEnabled = true
            settings.allowFileAccess = false

            webViewClient = TiddlerWebViewInterceptor(storage) { assetName ->
                runCatching { assets.open(assetName).use { it.readBytes() } }.getOrNull()
            }
            addJavascriptInterface(
                TiddlerWebViewInterceptor.JsBridge(storage),
                "AndroidTiddlyBridge"
            )
        }

        setContentView(webView)
        TiddlerSyncWorker.schedulePeriodicSync(applicationContext)
        webView.loadUrl(TiddlerWebViewInterceptor.BOOT_URL)
    }

    override fun onDestroy() {
        webView.destroy()
        storage.close()
        super.onDestroy()
    }
}
