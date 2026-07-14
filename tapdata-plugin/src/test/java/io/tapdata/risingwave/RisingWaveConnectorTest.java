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
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
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
        assertArrayEquals(new int[]{3, 0, 0}, RisingWaveConnector.parseRisingWaveVersion(
                "PostgreSQL 13.14.0-RisingWave-3.0.0 (abc123)"));
        assertArrayEquals(new int[]{3, 2, 1}, RisingWaveConnector.parseRisingWaveVersion(
                "PostgreSQL 13.14.0-RisingWave-3.2.1-alpha"));
    }

    @Test
    void acceptsSupportedWebSocketVersions() {
        assertTrue(RisingWaveConnector.supportsWebSocketIngest(
                "PostgreSQL 13.14.0-RisingWave-3.0.0"));
        assertTrue(RisingWaveConnector.supportsWebSocketIngest(
                "PostgreSQL 13.14.0-RisingWave-4.1.0"));
    }

    @Test
    void rejectsUnsupportedOrUnknownWebSocketVersions() {
        assertFalse(RisingWaveConnector.supportsWebSocketIngest(
                "PostgreSQL 13.14.0-RisingWave-2.9.9"));
        assertFalse(RisingWaveConnector.supportsWebSocketIngest("PostgreSQL 16.2"));
        assertFalse(RisingWaveConnector.supportsWebSocketIngest(null));
        assertNull(RisingWaveConnector.parseRisingWaveVersion("unknown"));
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
    void quotesSqlIdentifiersIncludingEmbeddedQuotes() {
        org.junit.jupiter.api.Assertions.assertEquals("\"simple\"",
                RisingWaveConnector.quoteIdentifier("simple"));
        org.junit.jupiter.api.Assertions.assertEquals("\"odd\"\"name\"",
                RisingWaveConnector.quoteIdentifier("odd\"name"));
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
        assertEquals("integer", RisingWaveConnector.canonicalRisingWaveType("int4"));
        assertEquals("varchar", RisingWaveConnector.canonicalRisingWaveType("text"));
        assertEquals("varchar", RisingWaveConnector.canonicalRisingWaveType("character varying"));
        assertEquals("numeric", RisingWaveConnector.canonicalRisingWaveType("numeric(20, 4)"));
        assertEquals("timestamp with time zone",
                RisingWaveConnector.canonicalRisingWaveType("timestamptz"));
    }
}
