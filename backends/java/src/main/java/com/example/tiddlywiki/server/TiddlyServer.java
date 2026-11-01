package com.example.tiddlywiki.server;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.javalin.Javalin;
import io.javalin.http.HttpStatus;
import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Minimal TiddlyWiki tiddler-syncing backend in Java 21 using Javalin 6 and SQLite JDBC.
 *
 * <p>Implements TiddlyWiki's {@code tiddlyweb} HTTP protocol, content-addressed binary blob storage
 * ({@code /files/{sha256}}), and a batch delta sync endpoint ({@code /api/sync}) for the Android
 * {@code WorkManager} periodic sync client.
 */
public final class TiddlyServer {

    private static final Set<String> CORE_TIDDLER_KEYS = Set.of(
            "title", "revision", "modified", "type", "text", "bag");

    private static final DateTimeFormatter TW_TIMESTAMP_FMT =
            DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS").withZone(ZoneOffset.UTC);

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

    private final Path dataDir;
    private final String jdbcUrl;
    private final Path tiddlerBackupDir;
    private final Path blobBackupDir;
    private final Path schemaPath;
    private final Object writeLock = new Object();

    public TiddlyServer(Path dataDir, Path schemaPath) {
        this.dataDir = dataDir;
        this.jdbcUrl = "jdbc:sqlite:" + dataDir.resolve("tiddlywiki.db").toAbsolutePath();
        this.tiddlerBackupDir = dataDir.resolve("backup").resolve("tiddlers");
        this.blobBackupDir = dataDir.resolve("backup").resolve("blobs");
        this.schemaPath = schemaPath;
        initStorage();
    }

    /** Initialize SQLite WAL database and backup directories. */
    public void initStorage() {
        try {
            Files.createDirectories(tiddlerBackupDir);
            Files.createDirectories(blobBackupDir);
            String schemaSql = Files.readString(schemaPath, StandardCharsets.UTF_8);
            try (Connection conn = connect();
                 Statement stmt = conn.createStatement()) {
                for (String statement : schemaSql.split(";")) {
                    String trimmed = statement.trim();
                    if (!trimmed.isEmpty()) {
                        stmt.execute(trimmed);
                    }
                }
            }
        } catch (IOException | SQLException e) {
            throw new IllegalStateException("Failed to initialize storage", e);
        }
    }

    /** Return tiddlyweb server status metadata. */
    public Map<String, Object> getStatus() {
        return Map.of(
                "username", "user",
                "space", Map.of("recipe", "default"),
                "tiddlywiki_version", "5.3.5");
    }

    /** Return all non-deleted tiddlers omitting the heavy {@code text} field. */
    public List<Map<String, Object>> listSkinnyTiddlers() throws SQLException, IOException {
        String sql = """
                SELECT title, revision, modified, type, fields_json, blob_hash
                FROM tiddlers
                WHERE deleted = 0
                ORDER BY modified DESC
                """;
        List<Map<String, Object>> result = new ArrayList<>();
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                result.add(rowToTiddler(rs, false));
            }
        }
        return result;
    }

    /** Fetch a single full tiddler (including {@code text}) by title. */
    public Map<String, Object> getTiddler(String title) throws SQLException, IOException {
        String sql = """
                SELECT title, revision, modified, type, fields_json, text, blob_hash
                FROM tiddlers
                WHERE title = ? AND deleted = 0
                """;
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, title);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return rowToTiddler(rs, true);
            }
        }
    }

    /** Upsert a tiddler in SQLite and write its human-readable JSON backup. */
    public int putTiddler(String title, Map<String, Object> payload)
            throws SQLException, IOException {
        synchronized (writeLock) {
            String modified = String.valueOf(
                    payload.getOrDefault("modified", currentTwTimestamp()));
            String type = String.valueOf(
                    payload.getOrDefault("type", "text/vnd.tiddlywiki"));
            String text = payload.get("text") != null
                    ? String.valueOf(payload.get("text"))
                    : null;

            Map<String, Object> extraFields = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : payload.entrySet()) {
                if (!CORE_TIDDLER_KEYS.contains(entry.getKey())) {
                    extraFields.put(entry.getKey(), entry.getValue());
                }
            }

            String blobHash = null;
            Object canonicalUri = extraFields.get("_canonical_uri");
            if (canonicalUri instanceof String uri && uri.startsWith("files/")) {
                blobHash = uri.substring("files/".length());
            } else if (payload.get("blob_hash") instanceof String bh && !bh.isEmpty()) {
                blobHash = bh;
            }

            String fieldsJson = MAPPER.writeValueAsString(extraFields);
            int nextRev;

            try (Connection conn = connect()) {
                nextRev = nextRevision(conn);
                String upsertSql = """
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
                        """;
                try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
                    ps.setString(1, title);
                    ps.setInt(2, nextRev);
                    ps.setString(3, modified);
                    ps.setString(4, type);
                    ps.setString(5, fieldsJson);
                    if (text != null) {
                        ps.setString(6, text);
                    } else {
                        ps.setNull(6, Types.VARCHAR);
                    }
                    if (blobHash != null) {
                        ps.setString(7, blobHash);
                    } else {
                        ps.setNull(7, Types.VARCHAR);
                    }
                    ps.executeUpdate();
                }
            }

            Map<String, Object> fullTiddler = new LinkedHashMap<>(extraFields);
            fullTiddler.put("title", title);
            fullTiddler.put("revision", nextRev);
            fullTiddler.put("modified", modified);
            fullTiddler.put("type", type);
            fullTiddler.put("bag", "default");
            if (text != null) {
                fullTiddler.put("text", text);
            }
            writeTiddlerBackup(title, fullTiddler, false);
            return nextRev;
        }
    }

    /** Soft-delete a tiddler via tombstone revision and update its backup. */
    public boolean deleteTiddler(String title) throws SQLException, IOException {
        synchronized (writeLock) {
            int nextRev;
            String nowTw = currentTwTimestamp();
            try (Connection conn = connect()) {
                try (PreparedStatement checkPs = conn.prepareStatement(
                        "SELECT deleted FROM tiddlers WHERE title = ?")) {
                    checkPs.setString(1, title);
                    try (ResultSet rs = checkPs.executeQuery()) {
                        if (!rs.next() || rs.getInt("deleted") == 1) {
                            return false;
                        }
                    }
                }
                nextRev = nextRevision(conn);
                try (PreparedStatement updatePs = conn.prepareStatement("""
                        UPDATE tiddlers
                        SET revision = ?, modified = ?, text = NULL, deleted = 1, dirty = 0
                        WHERE title = ?
                        """)) {
                    updatePs.setInt(1, nextRev);
                    updatePs.setString(2, nowTw);
                    updatePs.setString(3, title);
                    updatePs.executeUpdate();
                }
            }

            writeTiddlerBackup(
                    title,
                    Map.of(
                            "title", title,
                            "revision", nextRev,
                            "modified", nowTw,
                            "deleted", true),
                    true);
            return true;
        }
    }

    /** Retrieve a binary blob and its MIME type by SHA-256 hash. */
    public BlobRecord getBlob(String sha256Hash) throws SQLException, IOException {
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT mime_type, data FROM blobs WHERE sha256 = ?")) {
            ps.setString(1, sha256Hash);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new BlobRecord(rs.getString("mime_type"), rs.getBytes("data"));
                }
            }
        }
        Path backupPath = blobBackupDir.resolve(sha256Hash);
        if (Files.isRegularFile(backupPath)) {
            return new BlobRecord("application/octet-stream", Files.readAllBytes(backupPath));
        }
        return null;
    }

    /** Verify SHA-256 and store a binary blob in SQLite and backup disk. */
    public String putBlob(String expectedSha256, String mimeType, byte[] data)
            throws SQLException, IOException {
        String actualSha256 = sha256Hex(data);
        if (expectedSha256 != null
                && !expectedSha256.isEmpty()
                && !expectedSha256.equals(actualSha256)) {
            throw new IllegalArgumentException(
                    "SHA-256 mismatch: expected " + expectedSha256 + ", got " + actualSha256);
        }

        synchronized (writeLock) {
            try (Connection conn = connect();
                 PreparedStatement ps = conn.prepareStatement("""
                         INSERT INTO blobs (sha256, mime_type, size_bytes, data)
                         VALUES (?, ?, ?, ?)
                         ON CONFLICT(sha256) DO NOTHING
                         """)) {
                ps.setString(1, actualSha256);
                ps.setString(2, mimeType);
                ps.setInt(3, data.length);
                ps.setBytes(4, data);
                ps.executeUpdate();
            }
            writeBlobBackup(actualSha256, data);
        }
        return actualSha256;
    }

    /** Apply client delta changes and return server changes since {@code sinceRevision}. */
    @SuppressWarnings("unchecked")
    public Map<String, Object> syncBatch(Map<String, Object> requestPayload)
            throws SQLException, IOException {
        int sinceRevision = ((Number) requestPayload.getOrDefault("sinceRevision", 0)).intValue();
        int limit = ((Number) requestPayload.getOrDefault("limit", 100)).intValue();

        List<Map<String, Object>> upserted = (List<Map<String, Object>>)
                requestPayload.getOrDefault("upserted", List.of());
        for (Map<String, Object> tiddler : upserted) {
            Object title = tiddler.get("title");
            if (title != null) {
                putTiddler(String.valueOf(title), tiddler);
            }
        }

        List<String> deletedReq = (List<String>)
                requestPayload.getOrDefault("deleted", List.of());
        for (String deletedTitle : deletedReq) {
            deleteTiddler(deletedTitle);
        }

        List<Map<String, Object>> changed = new ArrayList<>();
        List<String> deleted = new ArrayList<>();
        int currentRev;

        try (Connection conn = connect()) {
            String deltaSql = """
                    SELECT title, revision, modified, type, fields_json, text, blob_hash, deleted
                    FROM tiddlers
                    WHERE revision > ?
                    ORDER BY revision ASC
                    LIMIT ?
                    """;
            try (PreparedStatement ps = conn.prepareStatement(deltaSql)) {
                ps.setInt(1, sinceRevision);
                ps.setInt(2, limit);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        if (rs.getInt("deleted") == 1) {
                            deleted.add(rs.getString("title"));
                        } else {
                            changed.add(rowToTiddler(rs, true));
                        }
                    }
                }
            }
            currentRev = nextRevision(conn) - 1;
        }

        return Map.of(
                "currentRevision", currentRev,
                "changed", changed,
                "deleted", deleted);
    }

    /** Write an atomic human-readable JSON snapshot of a tiddler to {@code backup/tiddlers/}. */
    public Path writeTiddlerBackup(
            String title, Map<String, Object> tiddlerObj, boolean deleted) throws IOException {
        String safeName = URLEncoder.encode(title, StandardCharsets.UTF_8);
        Path activePath = tiddlerBackupDir.resolve(safeName + ".json");
        Path deletedPath = tiddlerBackupDir.resolve(safeName + ".deleted.json");

        Path targetPath = deleted ? deletedPath : activePath;
        Path stalePath = deleted ? activePath : deletedPath;
        Files.deleteIfExists(stalePath);

        Path tempPath = targetPath.resolveSibling(targetPath.getFileName() + ".tmp");
        String jsonText = MAPPER.writeValueAsString(tiddlerObj) + "\n";
        Files.writeString(tempPath, jsonText, StandardCharsets.UTF_8);
        Files.move(tempPath, targetPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        return targetPath;
    }

    /** Write an atomic raw binary file to {@code backup/blobs/<sha256>}. */
    public Path writeBlobBackup(String sha256Hash, byte[] data) throws IOException {
        Path targetPath = blobBackupDir.resolve(sha256Hash);
        if (!Files.exists(targetPath)) {
            Path tempPath = targetPath.resolveSibling(sha256Hash + ".tmp");
            Files.write(tempPath, data);
            Files.move(tempPath, targetPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        }
        return targetPath;
    }

    private Connection connect() throws SQLException {
        Connection conn = DriverManager.getConnection(jdbcUrl);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("PRAGMA journal_mode = WAL;");
            stmt.execute("PRAGMA synchronous = NORMAL;");
        }
        return conn;
    }

    private static int nextRevision(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT COALESCE(MAX(revision), 0) + 1 AS next_rev FROM tiddlers")) {
            rs.next();
            return rs.getInt("next_rev");
        }
    }

    private static Map<String, Object> rowToTiddler(ResultSet rs, boolean includeText)
            throws SQLException, IOException {
        String fieldsJson = rs.getString("fields_json");
        Map<String, Object> fields = (fieldsJson != null && !fieldsJson.isEmpty())
                ? MAPPER.readValue(fieldsJson, new TypeReference<LinkedHashMap<String, Object>>() {})
                : new LinkedHashMap<>();

        fields.put("title", rs.getString("title"));
        fields.put("revision", rs.getInt("revision"));
        fields.put("modified", rs.getString("modified"));
        fields.put("type", rs.getString("type"));
        fields.put("bag", "default");

        String blobHash = rs.getString("blob_hash");
        if (blobHash != null && !blobHash.isEmpty() && !fields.containsKey("_canonical_uri")) {
            fields.put("_canonical_uri", "files/" + blobHash);
        }
        if (includeText) {
            String text = rs.getString("text");
            if (text != null) {
                fields.put("text", text);
            }
        }
        return fields;
    }

    private static String currentTwTimestamp() {
        return TW_TIMESTAMP_FMT.format(Instant.now());
    }

    private static String sha256Hex(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(data));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    public record BlobRecord(String mimeType, byte[] data) {}

    public static void main(String[] args) {
        Path dataDir = Path.of(System.getenv().getOrDefault("DATA_DIR", "../../data"));
        Path schemaPath = Path.of("../schema.sql");
        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "8080"));

        TiddlyServer server = new TiddlyServer(dataDir, schemaPath);

        Javalin app = Javalin.create(config -> {
            config.bundledPlugins.enableCors(cors -> cors.addRule(it -> it.anyHost()));
            config.http.gzipOnlyCompression();
        }).start(port);

        app.get("/status", ctx -> ctx.json(server.getStatus()));

        app.get("/recipes/default/tiddlers.json", ctx -> ctx.json(server.listSkinnyTiddlers()));

        app.get("/recipes/default/tiddlers/{title}", ctx -> {
            String title = URLDecoder.decode(ctx.pathParam("title"), StandardCharsets.UTF_8);
            Map<String, Object> tiddler = server.getTiddler(title);
            if (tiddler == null) {
                ctx.status(HttpStatus.NOT_FOUND).json(Map.of("error", "Not found"));
            } else {
                ctx.json(tiddler);
            }
        });

        app.put("/recipes/default/tiddlers/{title}", ctx -> {
            String title = URLDecoder.decode(ctx.pathParam("title"), StandardCharsets.UTF_8);
            Map<String, Object> payload = MAPPER.readValue(
                    ctx.bodyAsBytes(), new TypeReference<LinkedHashMap<String, Object>>() {});
            int rev = server.putTiddler(title, payload);
            String encodedTitle = URLEncoder.encode(title, StandardCharsets.UTF_8);
            ctx.header("ETag", "\"default/" + encodedTitle + "/" + rev + ":\"")
                    .status(HttpStatus.NO_CONTENT);
        });

        app.delete("/bags/default/tiddlers/{title}", ctx -> {
            String title = URLDecoder.decode(ctx.pathParam("title"), StandardCharsets.UTF_8);
            if (!server.deleteTiddler(title)) {
                ctx.status(HttpStatus.NOT_FOUND).json(Map.of("error", "Not found"));
            } else {
                ctx.status(HttpStatus.NO_CONTENT);
            }
        });

        app.get("/files/{sha256}", ctx -> {
            String sha256Hash = ctx.pathParam("sha256");
            BlobRecord blob = server.getBlob(sha256Hash);
            if (blob == null) {
                ctx.status(HttpStatus.NOT_FOUND).json(Map.of("error", "Blob not found"));
            } else {
                ctx.contentType(blob.mimeType()).result(blob.data());
            }
        });

        app.put("/files/{sha256}", ctx -> {
            String sha256Hash = ctx.pathParam("sha256");
            String mimeType = ctx.contentType() != null
                    ? ctx.contentType()
                    : "application/octet-stream";
            try {
                String storedHash = server.putBlob(sha256Hash, mimeType, ctx.bodyAsBytes());
                ctx.status(HttpStatus.CREATED)
                        .json(Map.of("sha256", storedHash, "uri", "files/" + storedHash));
            } catch (IllegalArgumentException e) {
                ctx.status(HttpStatus.BAD_REQUEST).json(Map.of("error", e.getMessage()));
            }
        });

        app.post("/api/sync", ctx -> {
            Map<String, Object> payload = MAPPER.readValue(
                    ctx.bodyAsBytes(), new TypeReference<LinkedHashMap<String, Object>>() {});
            ctx.json(server.syncBatch(payload));
        });
    }
}
