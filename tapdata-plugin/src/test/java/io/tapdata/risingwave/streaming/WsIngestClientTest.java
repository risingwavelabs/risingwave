package io.tapdata.risingwave.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CancellationException;
import java.time.Instant;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WsIngestClientTest {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    @Test
    @SuppressWarnings("unchecked")
    void serializesNestedValuesControlCharactersAndBytea() throws Exception {
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("message", "line1\nline2\u0001");
        record.put("attributes", Collections.singletonMap("enabled", true));
        record.put("values", Arrays.asList(1, "two"));
        record.put("payload", new byte[]{0x00, 0x0f, (byte) 0xff});

        String json = WsIngestClient.buildBatchPayloadJson(7,
                Collections.singletonList(new WsIngestClient.DmlOperation("insert", null, record)));
        Map<String, Object> payload = JSON_MAPPER.readValue(json, Map.class);
        assertEquals(7L, ((Number) payload.get("dml_batch_id")).longValue());

        List<Map<String, Object>> items = (List<Map<String, Object>>) payload.get("items");
        Map<String, Object> data = (Map<String, Object>) items.get(0).get("data");
        assertEquals("line1\nline2\u0001", data.get("message"));
        assertEquals("\\\\x000fff", data.get("payload"));
        assertTrue((Boolean) ((Map<String, Object>) data.get("attributes")).get("enabled"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void usesBeforeImageForDeleteAndAfterImageForUpsert() throws Exception {
        Map<String, Object> before = Collections.singletonMap("id", 1);
        Map<String, Object> after = Collections.singletonMap("id", 2);
        String json = WsIngestClient.buildBatchPayloadJson(1, Arrays.asList(
                new WsIngestClient.DmlOperation("delete", before, after),
                new WsIngestClient.DmlOperation("update", before, after)));

        Map<String, Object> payload = JSON_MAPPER.readValue(json, Map.class);
        List<Map<String, Object>> items = (List<Map<String, Object>>) payload.get("items");
        assertEquals(1L, ((Number) ((Map<String, Object>) items.get(0).get("data")).get("id")).longValue());
        assertEquals(2L, ((Number) ((Map<String, Object>) items.get(1).get("data")).get("id")).longValue());
    }

    @Test
    @SuppressWarnings("unchecked")
    void closeFailsPendingAcknowledgementsImmediately() throws Exception {
        WsIngestClient client = new WsIngestClient("ws://localhost:4560", "dev", "public", "t", "");
        java.lang.reflect.Field pendingField = WsIngestClient.class.getDeclaredField("pending");
        pendingField.setAccessible(true);
        Map<Long, CompletableFuture<Void>> pending =
                (Map<Long, CompletableFuture<Void>>) pendingField.get(client);
        CompletableFuture<Void> acknowledgement = new CompletableFuture<>();
        pending.put(1L, acknowledgement);

        client.close();

        assertTrue(acknowledgement.isCompletedExceptionally());
        assertTrue(pending.isEmpty());
    }

    @Test
    void closeRejectsNewBatchesImmediately() {
        WsIngestClient client = new WsIngestClient("ws://localhost:4560", "dev", "public", "t", "");
        client.close();

        List<CompletableFuture<Void>> futures = client.sendBatch(Collections.singletonList(
                new WsIngestClient.DmlOperation(
                        "insert", null, Collections.singletonMap("id", 1))));

        assertEquals(1, futures.size());
        assertTrue(futures.get(0).isCompletedExceptionally());
        org.junit.jupiter.api.Assertions.assertThrows(
                CancellationException.class, futures.get(0)::join);
    }

    @Test
    @SuppressWarnings("unchecked")
    void serializesJavaTimeValuesAsIsoStrings() throws Exception {
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("instant", Instant.parse("2026-07-13T08:00:00Z"));
        record.put("date", LocalDate.of(2026, 7, 13));

        String json = WsIngestClient.buildBatchPayloadJson(8,
                Collections.singletonList(new WsIngestClient.DmlOperation("insert", null, record)));
        Map<String, Object> payload = JSON_MAPPER.readValue(json, Map.class);
        List<Map<String, Object>> items = (List<Map<String, Object>>) payload.get("items");
        Map<String, Object> data = (Map<String, Object>) items.get(0).get("data");

        assertEquals("2026-07-13T08:00:00Z", data.get("instant"));
        assertEquals("2026-07-13", data.get("date"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void invalidAckFailsPendingAcknowledgementsInsteadOfTimingOut() throws Exception {
        WsIngestClient client = new WsIngestClient("ws://localhost:4560", "dev", "public", "t", "");
        java.lang.reflect.Field pendingField = WsIngestClient.class.getDeclaredField("pending");
        pendingField.setAccessible(true);
        Map<Long, CompletableFuture<Void>> pending =
                (Map<Long, CompletableFuture<Void>>) pendingField.get(client);
        CompletableFuture<Void> acknowledgement = new CompletableFuture<>();
        pending.put(1L, acknowledgement);

        java.lang.reflect.Method handleMessage =
                WsIngestClient.class.getDeclaredMethod("handleMessage", String.class);
        handleMessage.setAccessible(true);
        handleMessage.invoke(client, "{\"ack\":\"1\"}");

        assertTrue(acknowledgement.isCompletedExceptionally());
        assertTrue(pending.isEmpty());
    }
}
