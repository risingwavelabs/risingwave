package io.tapdata.risingwave.streaming;

import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.risingwave.RisingWaveConnector;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * WebSocket client for the RisingWave streaming ingest endpoint.
 *
 * Connects to: {ingestEndpoint}/ingest/{database}/{schema}/{table}
 *
 * Client → Server messages (JSON text frames):
 *   {"type":"init","timestamp":1760000000000}
 *   {
 *      "dml_batch_id":1,
 *      "items":[
 *        {"op":"upsert","data":{"id":1,"name":"foo"}},
 *        {"op":"delete","data":{"id":1,"name":"foo"}}
 *      ]
 *   }
 *
 * Server → Client messages:
 *   {"ack":1}
 *   {"fatal":"..."}
 */
public class WsIngestClient implements AutoCloseable {

    private static final String SIGNATURE_HEADER = "x-rw-signature";

    private final String wsUri;
    private final String webhookSecret;

    /** Monotonically increasing per-stream DML batch identifier. */
    private final AtomicLong dmlBatchIdGen = new AtomicLong(1);

    /** Maps dml_batch_id → future to complete when the server acks it. */
    private final ConcurrentHashMap<Long, CompletableFuture<Void>> pending = new ConcurrentHashMap<>();

    private volatile WebSocket webSocket;
    private volatile boolean closed = false;

    /** Guard concurrent sendText calls — Java WebSocket prohibits concurrent sends. */
    private final Object sendLock = new Object();

    public WsIngestClient(String ingestEndpoint, String database, String schema, String table, String webhookSecret) {
        String normalizedEndpoint = trimTrailingSlash(ingestEndpoint);
        this.wsUri = normalizedEndpoint
                + "/ingest/" + urlEncode(database)
                + "/" + urlEncode(schema)
                + "/" + urlEncode(table);
        this.webhookSecret = webhookSecret == null ? "" : webhookSecret;
    }

    /** Establish the WebSocket connection and send the signed init frame. */
    public void connect() throws Exception {
        RisingWaveConnector.debugLog("WsIngestClient.connect uri=" + wsUri);
        HttpClient client = HttpClient.newHttpClient();
        String initJson = buildInitJson(System.currentTimeMillis());
        String signature = signPayload(initJson);

        java.net.http.WebSocket.Builder builder = client.newWebSocketBuilder();
        if (!signature.isEmpty()) {
            builder.header(SIGNATURE_HEADER, signature);
        }

        this.webSocket = builder
                .buildAsync(URI.create(wsUri), new IngestListener())
                .get(15, TimeUnit.SECONDS);
        this.webSocket.sendText(initJson, true).get(10, TimeUnit.SECONDS);
        RisingWaveConnector.debugLog("WsIngestClient.connect done uri=" + wsUri + " init=" + initJson);
    }

    public List<CompletableFuture<Void>> sendBatch(List<DmlOperation> operations) {
        List<CompletableFuture<Void>> ackFutures = new ArrayList<>();
        if (operations == null || operations.isEmpty()) {
            return ackFutures;
        }

        long dmlBatchId = -1;
        CompletableFuture<Void> ackFuture = new CompletableFuture<>();
        ackFutures.add(ackFuture);
        try {
            synchronized (sendLock) {
                dmlBatchId = dmlBatchIdGen.getAndIncrement();
                String payloadJson = buildBatchPayloadJson(dmlBatchId, operations);
                pending.put(dmlBatchId, ackFuture);

                RisingWaveConnector.debugLog("WsIngestClient.sendBatch dmlBatchId=" + dmlBatchId + " json=" + payloadJson);
                webSocket.sendText(payloadJson, true).get(10, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            RisingWaveConnector.debugLog("WsIngestClient.sendBatch ERROR dmlBatchId=" + dmlBatchId + ": "
                    + e.getClass().getName() + ": " + e.getMessage());
            CompletableFuture<Void> future = pending.remove(dmlBatchId);
            if (future != null) {
                future.completeExceptionally(e);
            } else {
                ackFuture.completeExceptionally(e);
            }
        }

        return ackFutures;
    }

    @Override
    public void close() {
        closed = true;
        WebSocket ws = this.webSocket;
        if (ws != null && !ws.isOutputClosed()) {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
        }
    }

    private class IngestListener implements WebSocket.Listener {
        private final StringBuilder buf = new StringBuilder();

        @Override
        public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
            buf.append(data);
            if (last) {
                handleMessage(buf.toString());
                buf.delete(0, buf.length());
            }
            ws.request(1);
            return null;
        }

        @Override
        public void onError(WebSocket ws, Throwable error) {
            RisingWaveConnector.debugLog("WsIngestClient.onError uri=" + wsUri + ": "
                    + error.getClass().getName() + ": " + error.getMessage());
            failAll(new RuntimeException("WebSocket error: " + error.getMessage(), error));
        }

        @Override
        public CompletionStage<?> onClose(WebSocket ws, int statusCode, String reason) {
            RisingWaveConnector.debugLog("WsIngestClient.onClose uri=" + wsUri
                    + " statusCode=" + statusCode + " reason=" + reason + " closed=" + closed);
            if (!closed) {
                failAll(new RuntimeException("WebSocket closed unexpectedly (code=" + statusCode + "): " + reason));
            }
            return null;
        }
    }

    private void handleMessage(String json) {
        RisingWaveConnector.debugLog("WsIngestClient.recv json=" + json);
        if (json.contains("\"ack\"")) {
            List<Long> ids = parseAckIds(json);
            for (Long id : ids) {
                CompletableFuture<Void> f = pending.remove(id);
                if (f != null) f.complete(null);
            }
        } else if (json.contains("\"error\"")) {
            long dmlId = parseErrorDmlBatchId(json);
            String msg = parseErrorMessage(json);
            CompletableFuture<Void> f = pending.remove(dmlId);
            if (f != null) {
                f.completeExceptionally(new RuntimeException("DML error (dml_batch_id=" + dmlId + "): " + msg));
            }
        } else if (json.contains("\"fatal\"")) {
            String msg = parseFatal(json);
            failAll(new RuntimeException("Fatal ingest error: " + msg));
        }
    }

    private void failAll(RuntimeException ex) {
        for (CompletableFuture<Void> f : pending.values()) {
            f.completeExceptionally(ex);
        }
        pending.clear();
    }

    private String buildInitJson(long timestampMs) {
        return "{\"type\":\"init\",\"timestamp\":" + timestampMs + "}";
    }

    private String buildDmlJson(String op,
                                Map<String, Object> before,
                                Map<String, Object> after) {
        String normalizedOp = "delete".equalsIgnoreCase(op) ? "delete" : "upsert";
        Map<String, Object> data = "delete".equals(normalizedOp)
                ? (before != null ? before : after)
                : (after != null ? after : before);

        StringBuilder sb = new StringBuilder(256);
        sb.append("{\"op\":\"").append(normalizedOp).append("\"");
        if (data != null) {
            sb.append(",\"data\":").append(mapToJson(data));
        }
        sb.append("}");
        return sb.toString();
    }

    private String buildBatchPayloadJson(long dmlBatchId, List<DmlOperation> operations) {
        StringBuilder sb = new StringBuilder(operations.size() * 128);
        sb.append("{\"dml_batch_id\":").append(dmlBatchId).append(",\"items\":[");
        boolean first = true;
        for (DmlOperation operation : operations) {
            if (!first) sb.append(",");
            first = false;
            sb.append(buildDmlJson(operation.op, operation.before, operation.after));
        }
        sb.append("]}");
        return sb.toString();
    }

    private String signPayload(String payloadJson) throws Exception {
        if (webhookSecret.isEmpty()) {
            return "";
        }
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(webhookSecret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        byte[] digest = mac.doFinal(payloadJson.getBytes(StandardCharsets.UTF_8));
        return "sha256=" + bytesToHex(digest);
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }

    private static String mapToJson(Map<String, Object> map) {
        return mapToJsonObject(map);
    }

    private static String mapToJsonObject(Map<?, ?> map) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!first) sb.append(",");
            first = false;
            String key = entry.getKey() == null ? "null" : entry.getKey().toString();
            sb.append("\"").append(escapeJson(key)).append("\":");
            sb.append(valueToJson(entry.getValue()));
        }
        sb.append("}");
        return sb.toString();
    }

    private static String valueToJson(Object value) {
        if (value == null) return "null";
        if (value instanceof RawJson) {
            return ((RawJson) value).json;
        }
        if (value instanceof Boolean) return value.toString();
        if (value instanceof Number) {
            if (value instanceof Double && !Double.isFinite((Double) value)) {
                return "\"" + value + "\"";
            }
            if (value instanceof Float && !Float.isFinite((Float) value)) {
                return "\"" + value + "\"";
            }
            if (value instanceof BigDecimal) {
                return ((BigDecimal) value).toPlainString();
            }
            return value.toString();
        }
        if (value instanceof byte[]) {
            return "\"" + toByteaHex((byte[]) value) + "\"";
        }
        if (value instanceof DateTime) {
            Timestamp ts = ((DateTime) value).toTimestamp();
            if (ts == null) return "null";
            return "\"" + ts.toString() + "\"";
        }
        if (value instanceof OffsetDateTime) {
            return "\"" + DateTimeFormatter.ISO_OFFSET_DATE_TIME.format((OffsetDateTime) value) + "\"";
        }
        if (value instanceof ZonedDateTime) {
            return "\"" + DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(((ZonedDateTime) value).toOffsetDateTime()) + "\"";
        }
        if (value instanceof Instant) {
            return "\"" + DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(((Instant) value).atOffset(ZoneOffset.UTC)) + "\"";
        }
        if (value instanceof LocalDateTime) {
            return "\"" + value + "\"";
        }
        if (value instanceof LocalDate) {
            return "\"" + value + "\"";
        }
        if (value instanceof LocalTime) {
            return "\"" + value + "\"";
        }
        if (value instanceof java.sql.Date) {
            return "\"" + value.toString() + "\"";
        }
        if (value instanceof java.sql.Time) {
            return "\"" + value + "\"";
        }
        if (value instanceof java.util.Date) {
            java.sql.Timestamp ts = (value instanceof java.sql.Timestamp)
                    ? (java.sql.Timestamp) value
                    : new java.sql.Timestamp(((java.util.Date) value).getTime());
            return "\"" + ts.toString() + "\"";
        }
        if (value instanceof UUID) {
            return "\"" + value + "\"";
        }
        if (value instanceof CharSequence || value instanceof Character) {
            return "\"" + escapeJson(value.toString()) + "\"";
        }
        if (value instanceof Map<?, ?>) {
            return mapToJsonObject((Map<?, ?>) value);
        }
        if (value instanceof Collection<?>) {
            return collectionToJson((Collection<?>) value);
        }
        if (value.getClass().isArray()) {
            return arrayToJson(value);
        }
        return "\"" + escapeJson(value.toString()) + "\"";
    }

    public static RawJson rawJson(String json) {
        return new RawJson(json);
    }

    private static String collectionToJson(Collection<?> values) {
        StringBuilder sb = new StringBuilder("[");
        boolean first = true;
        for (Object value : values) {
            if (!first) sb.append(",");
            first = false;
            sb.append(valueToJson(value));
        }
        sb.append("]");
        return sb.toString();
    }

    private static String arrayToJson(Object array) {
        StringBuilder sb = new StringBuilder("[");
        int length = Array.getLength(array);
        for (int i = 0; i < length; i++) {
            if (i > 0) sb.append(",");
            sb.append(valueToJson(Array.get(array, i)));
        }
        sb.append("]");
        return sb.toString();
    }

    private static String toByteaHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2 + 2);
        sb.append("\\\\x");
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    static List<Long> parseAckIds(String json) {
        List<Long> ids = new ArrayList<>();
        int ackIdx = json.indexOf("\"ack\"");
        if (ackIdx < 0) return ids;
        int colon = json.indexOf(":", ackIdx);
        if (colon < 0) return ids;
        int numStart = colon + 1;
        while (numStart < json.length() && json.charAt(numStart) == ' ') numStart++;
        int numEnd = numStart;
        while (numEnd < json.length() && (Character.isDigit(json.charAt(numEnd)) || json.charAt(numEnd) == '-')) numEnd++;
        try {
            ids.add(Long.parseLong(json.substring(numStart, numEnd)));
        } catch (NumberFormatException ignored) {
        }
        return ids;
    }

    public static class DmlOperation {
        private final String op;
        private final Map<String, Object> before;
        private final Map<String, Object> after;

        public DmlOperation(String op, Map<String, Object> before, Map<String, Object> after) {
            this.op = op;
            this.before = before;
            this.after = after;
        }
    }

    public static final class RawJson {
        private final String json;

        private RawJson(String json) {
            this.json = json;
        }
    }

    static long parseErrorDmlBatchId(String json) {
        int idx = json.indexOf("\"dml_batch_id\"");
        if (idx < 0) idx = json.indexOf("\"dml_id\"");
        if (idx < 0) return -1;
        int colon = json.indexOf(":", idx);
        if (colon < 0) return -1;
        int numStart = colon + 1;
        while (numStart < json.length() && json.charAt(numStart) == ' ') numStart++;
        int numEnd = numStart;
        while (numEnd < json.length() && (Character.isDigit(json.charAt(numEnd)) || json.charAt(numEnd) == '-')) numEnd++;
        try {
            return Long.parseLong(json.substring(numStart, numEnd));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    static String parseErrorMessage(String json) {
        return parseStringField(json, "message");
    }

    static String parseFatal(String json) {
        return parseStringField(json, "fatal");
    }

    private static String parseStringField(String json, String field) {
        int idx = json.indexOf("\"" + field + "\"");
        if (idx < 0) return "";
        int colon = json.indexOf(":", idx);
        if (colon < 0) return "";
        int quoteStart = json.indexOf("\"", colon + 1);
        if (quoteStart < 0) return "";
        int quoteEnd = quoteStart + 1;
        while (quoteEnd < json.length()) {
            char c = json.charAt(quoteEnd);
            if (c == '\\') { quoteEnd += 2; continue; }
            if (c == '"') break;
            quoteEnd++;
        }
        return json.substring(quoteStart + 1, quoteEnd)
                .replace("\\\"", "\"")
                .replace("\\n", "\n")
                .replace("\\r", "\r")
                .replace("\\t", "\t")
                .replace("\\\\", "\\");
    }

    private static String trimTrailingSlash(String s) {
        if (s == null || s.isEmpty()) {
            return "ws://localhost:4560";
        }
        int end = s.length();
        while (end > 0 && s.charAt(end - 1) == '/') {
            end--;
        }
        return s.substring(0, end);
    }

    private static String urlEncode(String s) {
        try {
            return java.net.URLEncoder.encode(s, "UTF-8").replace("+", "%20");
        } catch (java.io.UnsupportedEncodingException e) {
            return s;
        }
    }
}
