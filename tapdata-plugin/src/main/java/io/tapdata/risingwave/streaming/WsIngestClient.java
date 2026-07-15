package io.tapdata.risingwave.streaming;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.tapdata.risingwave.RisingWaveConnector;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.time.temporal.TemporalAccessor;
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
    /**
     * Keep a margin below RisingWave/Axum's default 16 MiB single-frame limit. A batch may be
     * split into multiple ordered DML frames, each with its own durable ACK.
     */
    static final int MAX_BATCH_PAYLOAD_BYTES = 8 * 1024 * 1024;
    /** Shares the selector, executor, and connection pool across all table clients. */
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final String wsUri;
    private final String webhookSecret;

    /** Monotonically increasing per-stream DML batch identifier. */
    private final AtomicLong dmlBatchIdGen = new AtomicLong(1);

    /** Maps dml_batch_id → future to complete when the server acks it. */
    private final ConcurrentHashMap<Long, CompletableFuture<Void>> pending = new ConcurrentHashMap<>();

    private volatile WebSocket webSocket;
    private volatile boolean closed = false;
    private volatile RuntimeException terminalFailure;

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
        String initJson = buildInitJson(System.currentTimeMillis());
        String signature = signPayload(initJson);

        java.net.http.WebSocket.Builder builder = HTTP_CLIENT.newWebSocketBuilder();
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
        CompletableFuture<Void> ackFuture = null;
        try {
            synchronized (sendLock) {
                if (closed) {
                    throw new CancellationException("WebSocket ingest client closed");
                }
                if (terminalFailure != null) {
                    throw terminalFailure;
                }
                if (webSocket == null) {
                    throw new IllegalStateException("WebSocket ingest client is not connected");
                }
                for (List<DmlOperation> batch : splitBatches(operations, MAX_BATCH_PAYLOAD_BYTES)) {
                    dmlBatchId = dmlBatchIdGen.getAndIncrement();
                    String payloadJson = buildBatchPayloadJson(dmlBatchId, batch);
                    ackFuture = new CompletableFuture<>();
                    ackFutures.add(ackFuture);
                    pending.put(dmlBatchId, ackFuture);

                    RisingWaveConnector.debugLog("WsIngestClient.sendBatch dmlBatchId=" + dmlBatchId
                            + " items=" + batch.size() + " bytes="
                            + payloadJson.getBytes(StandardCharsets.UTF_8).length);
                    webSocket.sendText(payloadJson, true).get(10, TimeUnit.SECONDS);
                }
            }
        } catch (Exception e) {
            RisingWaveConnector.debugLog("WsIngestClient.sendBatch ERROR dmlBatchId=" + dmlBatchId + ": "
                    + e.getClass().getName() + ": " + e.getMessage());
            CompletableFuture<Void> future = pending.remove(dmlBatchId);
            if (future != null) {
                future.completeExceptionally(e);
            } else if (ackFuture != null) {
                ackFuture.completeExceptionally(e);
            } else {
                CompletableFuture<Void> failed = new CompletableFuture<>();
                failed.completeExceptionally(e);
                ackFutures.add(failed);
            }
        }

        return ackFutures;
    }

    @Override
    public void close() {
        WebSocket ws;
        synchronized (sendLock) {
            closed = true;
            failAllLocked(new CancellationException("WebSocket ingest client closed"));
            ws = this.webSocket;
            this.webSocket = null;
        }
        if (ws != null && !ws.isOutputClosed()) {
            try {
                ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
            } catch (Exception ignored) {
                ws.abort();
            }
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
        final JsonNode response;
        try {
            response = JSON_MAPPER.readTree(json);
        } catch (JsonProcessingException e) {
            failAll(new RuntimeException("Invalid WebSocket ingest response", e));
            return;
        }

        JsonNode ack = response.get("ack");
        if (ack != null) {
            if (!ack.isIntegralNumber() || !ack.canConvertToLong()) {
                failAll(new RuntimeException("Invalid WebSocket ingest ACK"));
                return;
            }
            long id = ack.longValue();
            CompletableFuture<Void> future = pending.remove(id);
            if (future == null) {
                failAll(new RuntimeException("Unexpected WebSocket ingest ACK for dml_batch_id=" + id));
            } else {
                future.complete(null);
            }
            return;
        }

        JsonNode fatal = response.get("fatal");
        if (fatal != null && fatal.isTextual()) {
            failAll(new RuntimeException("Fatal ingest error: " + fatal.textValue()));
            return;
        }

        // Older gateways may return a per-batch error object. RisingWave itself currently
        // sends only ack or fatal, but retaining this branch gives a useful targeted failure.
        JsonNode error = response.get("error");
        if (error != null) {
            JsonNode idNode = response.has("dml_batch_id")
                    ? response.get("dml_batch_id") : response.get("dml_id");
            JsonNode messageNode = response.get("message");
            if (idNode == null || !idNode.isIntegralNumber() || !idNode.canConvertToLong()) {
                failAll(new RuntimeException("Invalid WebSocket ingest error response"));
                return;
            }
            long id = idNode.longValue();
            String message = messageNode != null && messageNode.isTextual()
                    ? messageNode.textValue() : error.toString();
            CompletableFuture<Void> future = pending.remove(id);
            if (future == null) {
                failAll(new RuntimeException("Unexpected WebSocket ingest error for dml_batch_id=" + id));
            } else {
                future.completeExceptionally(
                        new RuntimeException("DML error (dml_batch_id=" + id + "): " + message));
            }
            return;
        }

        failAll(new RuntimeException("Unknown WebSocket ingest response"));
    }

    private void failAll(RuntimeException ex) {
        synchronized (sendLock) {
            failAllLocked(ex);
        }
    }

    private void failAllLocked(RuntimeException ex) {
        terminalFailure = ex;
        for (CompletableFuture<Void> f : pending.values()) {
            f.completeExceptionally(ex);
        }
        pending.clear();
    }

    private String buildInitJson(long timestampMs) {
        return "{\"type\":\"init\",\"timestamp\":" + timestampMs + "}";
    }

    private static Map<String, Object> buildDml(String op,
                                                Map<String, Object> before,
                                                Map<String, Object> after) {
        String normalizedOp = "delete".equalsIgnoreCase(op) ? "delete" : "upsert";
        Map<String, Object> data = "delete".equals(normalizedOp)
                ? (before != null ? before : after)
                : (after != null ? after : before);
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("op", normalizedOp);
        if (data != null) {
            item.put("data", normalizeJsonValues(data));
        }
        return item;
    }

    static String buildBatchPayloadJson(long dmlBatchId, List<DmlOperation> operations) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("dml_batch_id", dmlBatchId);
        List<Map<String, Object>> items = new ArrayList<>(operations.size());
        for (DmlOperation operation : operations) {
            items.add(buildDml(operation.op, operation.before, operation.after));
        }
        payload.put("items", items);
        try {
            return JSON_MAPPER.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Unable to serialize WebSocket ingest batch", e);
        }
    }

    /** Split ordered operations into payloads that remain below a single WebSocket frame limit. */
    static List<List<DmlOperation>> splitBatches(List<DmlOperation> operations, int maxPayloadBytes) {
        if (maxPayloadBytes <= 0) {
            throw new IllegalArgumentException("WebSocket batch payload limit must be positive");
        }
        List<List<DmlOperation>> batches = new ArrayList<>();
        List<DmlOperation> current = new ArrayList<>();
        for (DmlOperation operation : operations) {
            current.add(operation);
            int bytes = payloadByteLength(current);
            if (bytes <= maxPayloadBytes) {
                continue;
            }
            current.remove(current.size() - 1);
            if (current.isEmpty()) {
                throw new IllegalArgumentException("A single WebSocket DML operation is " + bytes
                        + " bytes, exceeding the " + maxPayloadBytes
                        + " byte frame safety limit; split the source record before replication");
            }
            batches.add(current);
            current = new ArrayList<>();
            current.add(operation);
            int singleOperationBytes = payloadByteLength(current);
            if (singleOperationBytes > maxPayloadBytes) {
                throw new IllegalArgumentException("A single WebSocket DML operation is "
                        + singleOperationBytes + " bytes, exceeding the " + maxPayloadBytes
                        + " byte frame safety limit; split the source record before replication");
            }
        }
        if (!current.isEmpty()) {
            batches.add(current);
        }
        return batches;
    }

    private static int payloadByteLength(List<DmlOperation> operations) {
        // Size with the longest possible batch ID so every generated payload remains
        // within the safety limit, even after the sequence grows.
        return buildBatchPayloadJson(Long.MAX_VALUE, operations).getBytes(StandardCharsets.UTF_8).length;
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

    private static Object normalizeJsonValues(Object value) {
        if (value instanceof byte[]) {
            return toByteaHex((byte[]) value);
        }
        if (value instanceof TemporalAccessor) {
            return value.toString();
        }
        if (value instanceof java.sql.Date || value instanceof java.sql.Time
                || value instanceof java.sql.Timestamp) {
            return value.toString();
        }
        if (value instanceof java.util.Date) {
            return ((java.util.Date) value).toInstant().toString();
        }
        if (value instanceof Map<?, ?>) {
            Map<String, Object> normalized = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                normalized.put(String.valueOf(entry.getKey()), normalizeJsonValues(entry.getValue()));
            }
            return normalized;
        }
        if (value instanceof Collection<?>) {
            List<Object> normalized = new ArrayList<>();
            for (Object element : (Collection<?>) value) {
                normalized.add(normalizeJsonValues(element));
            }
            return normalized;
        }
        return value;
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
