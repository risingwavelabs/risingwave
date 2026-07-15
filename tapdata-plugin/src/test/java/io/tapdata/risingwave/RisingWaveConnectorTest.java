package io.tapdata.risingwave;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapBinaryValue;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapJsonValue;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.schema.value.TapRawValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.risingwave.streaming.WsIngestClient;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RisingWaveConnectorTest {

    @Test
    void recognizesBothWebSocketModes() {
        assertTrue(RisingWaveConnector.isWebSocketMode(RisingWaveConnector.MODE_STREAMING));
        assertTrue(RisingWaveConnector.isWebSocketMode(RisingWaveConnector.MODE_STREAMING_JSONB));
        assertFalse(RisingWaveConnector.isWebSocketMode(RisingWaveConnector.MODE_JDBC));
    }

    @Test
    void parsesRisingWaveVersionFromPostgresCompatibleVersionString() {
        assertArrayEquals(new int[]{3, 0, 0}, RisingWaveConnectionTester.parseRisingWaveVersion(
                "PostgreSQL 13.14.0-RisingWave-3.0.0 (abc123)"));
        assertArrayEquals(new int[]{3, 2, 1}, RisingWaveConnectionTester.parseRisingWaveVersion(
                "PostgreSQL 13.14.0-RisingWave-3.2.1-alpha"));
        assertEquals("3.2.1", RisingWaveConnectionTester.parseRisingWaveVersionString(
                "PostgreSQL 13.14.0-RisingWave-3.2.1-alpha"));
        assertEquals("3.2.0", RisingWaveConnectionTester.parseRisingWaveVersionString(
                "PostgreSQL 13.14.0-RisingWave-3.2"));
    }

    @Test
    void acceptsSupportedWebSocketVersions() {
        assertTrue(RisingWaveConnectionTester.supportsWebSocketIngest(
                "PostgreSQL 13.14.0-RisingWave-3.0.0"));
        assertTrue(RisingWaveConnectionTester.supportsWebSocketIngest(
                "PostgreSQL 13.14.0-RisingWave-4.1.0"));
    }

    @Test
    void rejectsUnsupportedOrUnknownWebSocketVersions() {
        assertFalse(RisingWaveConnectionTester.supportsWebSocketIngest(
                "PostgreSQL 13.14.0-RisingWave-2.9.9"));
        assertFalse(RisingWaveConnectionTester.supportsWebSocketIngest("PostgreSQL 16.2"));
        assertFalse(RisingWaveConnectionTester.supportsWebSocketIngest(null));
        assertNull(RisingWaveConnectionTester.parseRisingWaveVersion("unknown"));
    }

    @Test
    void resolvesDefaultAndCustomWebSocketEndpoints() {
        assertEquals("ws://risingwave:4560",
                RisingWaveConnector.resolveIngestEndpoint(null, "risingwave"));
        assertEquals("ws://risingwave:4560",
                RisingWaveConnector.resolveIngestEndpoint("  ", "risingwave"));
        assertEquals("ws://risingwave:4560",
                RisingWaveConnector.resolveIngestEndpoint("ws://{Host}:4560", "risingwave"));
        assertEquals("wss://ingest.example.com",
                RisingWaveConnector.resolveIngestEndpoint("wss://ingest.example.com", "risingwave"));
    }

    @Test
    void reportsInvalidPortAsConnectionTestFailure() throws Throwable {
        DataMap config = DataMap.create()
                .kv("host", "127.0.0.1")
                .kv("port", "not-a-port")
                .kv("database", "dev");
        TapConnectionContext context = new TapConnectionContext(
                null, config, DataMap.create(), null);
        java.util.List<TestItem> items = new java.util.ArrayList<>();

        new RisingWaveConnector().connectionTest(context, items::add);

        assertEquals(1, items.size());
        assertEquals(TestItem.ITEM_CONNECTION, items.get(0).getItem());
        assertEquals(TestItem.RESULT_FAILED, items.get(0).getResult());
        assertTrue(items.get(0).getInformation().contains("Port must be a number"));
    }

    @Test
    void quotesSqlIdentifiersIncludingEmbeddedQuotes() {
        org.junit.jupiter.api.Assertions.assertEquals("\"simple\"",
                RisingWaveSql.quoteIdentifier("simple"));
        org.junit.jupiter.api.Assertions.assertEquals("\"odd\"\"name\"",
                RisingWaveSql.quoteIdentifier("odd\"name"));
        assertEquals("'it''s protected'",
                RisingWaveSql.quoteStringLiteral("it's protected"));
    }

    @Test
    void webhookValidationReferencesCatalogSecretInsteadOfInliningValue() {
        String clause = RisingWaveSql.webhookValidationClause("tapdata_secret", null);

        assertTrue(clause.contains("VALIDATE SECRET \"tapdata_secret\""));
        assertTrue(clause.contains("hmac(\"tapdata_secret\", payload"));
        assertFalse(clause.contains("your-secret-value"));
    }

    @Test
    void rejectsQualifiedOrUnsafeWebhookSecretNames() {
        IllegalArgumentException qualified = assertThrows(IllegalArgumentException.class,
                () -> RisingWaveWebhookSecret.prepare(
                        null, "public", "orders", "other.secret", "value"));
        assertTrue(qualified.getMessage().contains("unqualified"));

        IllegalArgumentException injected = assertThrows(IllegalArgumentException.class,
                () -> RisingWaveWebhookSecret.prepare(
                        null, "public", "orders", "secret; DROP TABLE orders", "value"));
        assertTrue(injected.getMessage().contains("identifier"));
    }

    @Test
    void registersTargetValueCodecs() {
        TapCodecsRegistry registry = new TapCodecsRegistry();
        new RisingWaveConnector().registerCapabilities(new ConnectorFunctions(), registry);

        assertTrue(registry.getCustomFromTapValueCodec(TapRawValue.class) != null);
        assertTrue(registry.getCustomFromTapValueCodec(TapMapValue.class) != null);
        assertTrue(registry.getCustomFromTapValueCodec(TapArrayValue.class) != null);
        assertTrue(registry.getCustomFromTapValueCodec(TapJsonValue.class) != null);
        assertTrue(registry.getCustomFromTapValueCodec(TapBinaryValue.class) != null);
        assertTrue(registry.getCustomFromTapValueCodec(TapTimeValue.class) != null);
        assertTrue(registry.getCustomFromTapValueCodec(TapDateTimeValue.class) != null);
        assertTrue(registry.getCustomFromTapValueCodec(TapDateValue.class) != null);
    }

    @Test
    void requiresDeleteBeforeUpsertOnlyWhenIdentityChanges() {
        TapTable keyedTable = new TapTable("target")
                .add(new TapField("id", "integer").isPrimaryKey(true).primaryKeyPos(1))
                .add(new TapField("name", "text"));
        java.util.Map<String, Object> before = new java.util.LinkedHashMap<>();
        before.put("id", 1);
        before.put("name", "before");
        java.util.Map<String, Object> sameKey = new java.util.LinkedHashMap<>();
        sameKey.put("id", 1);
        sameKey.put("name", "after");
        java.util.Map<String, Object> changedKey = new java.util.LinkedHashMap<>();
        changedKey.put("id", 2);
        changedKey.put("name", "after");

        assertFalse(RisingWaveConnector.requiresDeleteBeforeUpsert(keyedTable, before, sameKey));
        assertTrue(RisingWaveConnector.requiresDeleteBeforeUpsert(keyedTable, before, changedKey));
        TapTable keylessTable = new TapTable("keyless")
                .add(new TapField("id", "integer"))
                .add(new TapField("name", "text"));
        assertTrue(RisingWaveConnector.requiresDeleteBeforeUpsert(keylessTable, before, sameKey));
    }

    @Test
    void rejectsKeylessModelsForWebSocketStreaming() {
        TapTable keylessTable = new TapTable("keyless")
                .add(new TapField("id", "integer"))
                .add(new TapField("name", "text"));

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> RisingWaveConnector.validateStreamingPrimaryKey(keylessTable));
        assertTrue(error.getMessage().contains("requires a primary key"));
    }

    @Test
    void jsonbAppendOnlyAcceptsKeylessInsertsAndRejectsChanges() {
        TapTable keylessTable = new TapTable("keyless")
                .add(new TapField("id", "integer"))
                .add(new TapField("name", "text"));
        java.util.Map<String, Object> row = new java.util.LinkedHashMap<>();
        row.put("id", 1);
        row.put("name", "inserted");

        java.util.List<TapRecordEvent> inserts = java.util.Collections.singletonList(
                TapInsertRecordEvent.create().table("keyless").after(row));
        RisingWaveConnector.validateJsonbAppendOnlyEvents(inserts, keylessTable);

        IllegalArgumentException updateError = assertThrows(IllegalArgumentException.class,
                () -> RisingWaveConnector.validateJsonbAppendOnlyEvents(
                        java.util.Collections.singletonList(
                                TapUpdateRecordEvent.create().table("keyless").before(row).after(row)),
                        keylessTable));
        assertTrue(updateError.getMessage().contains("accepts only inserts"));

        IllegalArgumentException deleteError = assertThrows(IllegalArgumentException.class,
                () -> RisingWaveConnector.validateJsonbAppendOnlyEvents(
                        java.util.Collections.singletonList(
                                TapDeleteRecordEvent.create().table("keyless").before(row)),
                        keylessTable));
        assertTrue(deleteError.getMessage().contains("TapDeleteRecordEvent"));
    }

    @Test
    void canonicalizesEquivalentRisingWaveTypes() {
        assertEquals("integer", RisingWaveSql.canonicalType("int4"));
        assertEquals("varchar", RisingWaveSql.canonicalType("text"));
        assertEquals("varchar", RisingWaveSql.canonicalType("character varying"));
        assertEquals("numeric", RisingWaveSql.canonicalType("numeric(20, 4)"));
        assertEquals("timestamp with time zone",
                RisingWaveSql.canonicalType("timestamptz"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void evictsOnlyIdleClientsFromFullWebSocketCache() throws Throwable {
        RisingWaveConnector connector = new RisingWaveConnector();
        java.lang.reflect.Field field = RisingWaveConnector.class.getDeclaredField("wsClients");
        field.setAccessible(true);
        java.util.Map<String, Object> clients =
                (java.util.Map<String, Object>) field.get(connector);
        Class<?> entryClass = Class.forName(
                "io.tapdata.risingwave.RisingWaveConnector$WsClientEntry");
        java.lang.reflect.Constructor<?> constructor =
                entryClass.getDeclaredConstructor(WsIngestClient.class);
        constructor.setAccessible(true);

        for (int index = 0; index < RisingWaveConnector.MAX_CACHED_WS_CLIENTS; index++) {
            WsIngestClient client = new WsIngestClient(
                    "ws://127.0.0.1:4560", "dev", "public", "table_" + index, "");
            clients.put("table_" + index, constructor.newInstance(client));
        }
        java.lang.reflect.Field inUse = entryClass.getDeclaredField("inUse");
        inUse.setAccessible(true);
        inUse.setInt(clients.get("table_0"), 1);
        java.lang.reflect.Method evict = RisingWaveConnector.class
                .getDeclaredMethod("evictIdleWsClientIfFull");
        evict.setAccessible(true);
        evict.invoke(connector);

        assertEquals(RisingWaveConnector.MAX_CACHED_WS_CLIENTS - 1, clients.size());
        assertTrue(clients.containsKey("table_0"));
        assertFalse(clients.containsKey("table_1"));
        connector.stop(null);
    }
}
