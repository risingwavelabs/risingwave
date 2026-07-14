package io.tapdata.risingwave;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapDate;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapJson;
import io.tapdata.entity.schema.type.TapMap;
import io.tapdata.entity.schema.type.TapTime;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapBinaryValue;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapJsonValue;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.schema.value.TapRawValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.risingwave.streaming.WsIngestClient;

import java.sql.*;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Tapdata PDK connector for RisingWave streaming database.
 *
 * Supports three write modes:
 * <ul>
 *   <li><b>jdbc</b> — Standard PostgreSQL JDBC inserts/updates/deletes.
 *       Compatible with all RisingWave deployments.</li>
 *   <li><b>streaming</b> (default) — High-throughput WebSocket streaming DML over the
 *       RisingWave ingest endpoint.  Sends DML messages asynchronously and
 *       waits for per-epoch acks before advancing the offset.  Requires the
 *       RisingWave webhook/ingest service to be reachable on {@code ingestEndpoint}.</li>
 *   <li><b>streaming_jsonb</b> — Append-only WebSocket ingest into a single JSONB column.
 *       This mode accepts keyless models but rejects updates and deletes.</li>
 * </ul>
 */
@TapConnectorClass("spec_risingwave.json")
public class RisingWaveConnector implements TapConnector {
    private static final String TAG = TapLogger.getClassTag(RisingWaveConnector.class);

    static final String SCHEMA_TEST_ITEM = "schema";
    static final String INGEST_ENDPOINT_TEST_ITEM = "ingest_endpoint";
    static final String MINIMUM_WEBSOCKET_VERSION = "3.0.0";
    static final String MODE_JDBC = "jdbc";
    static final String MODE_STREAMING = "streaming";
    static final String MODE_STREAMING_JSONB = "streaming_jsonb";
    static final String JSONB_PAYLOAD_COLUMN = "data";
    private static final java.util.regex.Pattern RISINGWAVE_VERSION_PATTERN =
            java.util.regex.Pattern.compile("(?i)RisingWave[-/\\s]+v?(\\d+)\\.(\\d+)(?:\\.(\\d+))?");
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private Connection connection;
    private String schema;

    /** "jdbc", "streaming", or "streaming_jsonb" */
    private String ingestMode;

    /** Only used in streaming mode. Per-table WsIngestClient cache. */
    private final Map<String, WsIngestClient> wsClients = new LinkedHashMap<>();
    private String wsIngestEndpoint;
    private String wsDatabase;
    private String wsWebhookSecret;
    private final AtomicBoolean alive = new AtomicBoolean(false);

    // ---- TapNode lifecycle ----

    public static void debugLog(String msg) {
        TapLogger.debug(TAG, "{}", msg);
    }

    @Override
    public void init(TapConnectionContext context) throws Throwable {
        closeResources();
        debugLog("init() called");
        DataMap cfg = context.getConnectionConfig();
        this.schema = cfg.getString("schema");
        if (schema == null || schema.isEmpty()) {
            schema = "public";
        }
        this.ingestMode = cfg.getString("ingest_mode");
        if (ingestMode == null || ingestMode.isEmpty()) {
            ingestMode = MODE_STREAMING;
        }

        if (isWebSocketMode(ingestMode)) {
            this.wsIngestEndpoint = resolveIngestEndpoint(
                    cfg.getString("ingestEndpoint"), cfg.getString("host"));
            this.wsDatabase = cfg.getString("database");
            this.wsWebhookSecret = cfg.getString("webhookSecret");
            if (wsDatabase == null || wsDatabase.isEmpty()) wsDatabase = "dev";
            debugLog("init() streaming mode, ingestEndpoint=" + wsIngestEndpoint + " db=" + wsDatabase);
        }

        // Always open a JDBC connection for schema discovery and DDL operations. Resolve all
        // WebSocket configuration first so a configuration failure cannot leak this connection.
        this.connection = openConnection(context);
        alive.set(true);
        debugLog("init() done, schema=" + schema + " ingestMode=" + ingestMode);
    }

    @Override
    public void stop(TapConnectionContext context) throws Throwable {
        closeResources();
    }

    private synchronized void closeResources() {
        alive.set(false);
        List<WsIngestClient> clients = new ArrayList<>(wsClients.values());
        wsClients.clear();
        for (WsIngestClient client : clients) {
            try { client.close(); } catch (Exception ignored) {}
        }
        closeQuietly(connection);
        connection = null;
    }

    private void releaseExternal(TapConnectorContext context) {
        closeResources();
    }

    // ---- TapConnectorNode ----

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext context,
                                            Consumer<TestItem> consumer) throws Throwable {
        ConnectionOptions options = ConnectionOptions.create();
        DataMap cfg = context.getConnectionConfig();
        String mode = cfg.getString("ingest_mode");
        if (mode == null || mode.isEmpty()) {
            mode = MODE_STREAMING;
        }
        String schemaName = configuredSchema(cfg);
        options.setNamespaces(Collections.singletonList(schemaName));
        debugLog("connectionTest() start host=" + cfg.getString("host")
                + " port=" + cfg.getObject("port")
                + " database=" + cfg.getString("database")
                + " schema=" + schemaName
                + " ingestMode=" + mode);
        // Always test JDBC connectivity for schema discovery and DDL operations.
        try (Connection conn = openConnection(context)) {
            consumer.accept(new TestItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY,
                    "Connected to RisingWave"));
            testVersion(conn, mode, consumer);
            if (testSchema(conn, schemaName, consumer)) {
                if (isWebSocketMode(mode)) {
                    testStreamingWrite(conn, cfg, schemaName, consumer,
                            MODE_STREAMING_JSONB.equals(mode));
                } else {
                    testJdbcWritePrivilege(conn, schemaName, consumer);
                }
            } else {
                consumer.accept(new TestItem(TestItem.ITEM_WRITE, TestItem.RESULT_FAILED,
                        "Write access cannot be verified because schema \"" + schemaName + "\" is unavailable"));
                if (isWebSocketMode(mode)) {
                    consumer.accept(new TestItem(INGEST_ENDPOINT_TEST_ITEM, TestItem.RESULT_FAILED,
                            "WebSocket ingest cannot be verified because schema \""
                                    + schemaName + "\" is unavailable"));
                }
            }
        } catch (Exception e) {
            debugLog("connectionTest() JDBC ERROR: " + e.getClass().getName() + ": " + e.getMessage());
            consumer.accept(new TestItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED,
                    "Connection failed: " + e.getMessage()));
        }

        return options;
    }

    private void testVersion(Connection conn, String mode, Consumer<TestItem> consumer) {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT version()")) {
            String version = rs.next() ? rs.getString(1) : "unknown";
            debugLog("connectionTest() version=" + version);
            if (isWebSocketMode(mode) && !supportsWebSocketIngest(version)) {
                consumer.accept(new TestItem(TestItem.ITEM_VERSION, TestItem.RESULT_FAILED,
                        "WebSocket streaming requires RisingWave " + MINIMUM_WEBSOCKET_VERSION
                                + " or later; server reported: " + version));
            } else {
                consumer.accept(new TestItem(TestItem.ITEM_VERSION, TestItem.RESULT_SUCCESSFULLY, version));
            }
        } catch (Exception e) {
            debugLog("connectionTest() version ERROR: " + e.getClass().getName() + ": " + e.getMessage());
            consumer.accept(new TestItem(TestItem.ITEM_VERSION, TestItem.RESULT_FAILED,
                    "Version check failed: " + e.getMessage()));
        }
    }

    private boolean testSchema(Connection conn, String schemaName, Consumer<TestItem> consumer) {
        String sql = "SELECT count(*) FROM rw_catalog.rw_schemas WHERE name = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            try (ResultSet rs = ps.executeQuery()) {
                boolean exists = rs.next() && rs.getInt(1) > 0;
                if (exists) {
                    consumer.accept(new TestItem(SCHEMA_TEST_ITEM, TestItem.RESULT_SUCCESSFULLY,
                            "Schema \"" + schemaName + "\" exists"));
                    return true;
                }
            }
            consumer.accept(new TestItem(SCHEMA_TEST_ITEM, TestItem.RESULT_FAILED,
                    "Schema \"" + schemaName + "\" does not exist"));
        } catch (Exception e) {
            debugLog("connectionTest() schema ERROR: " + e.getClass().getName() + ": " + e.getMessage());
            consumer.accept(new TestItem(SCHEMA_TEST_ITEM, TestItem.RESULT_FAILED,
                    "Schema check failed: " + e.getMessage()));
        }
        return false;
    }

    private void testJdbcWritePrivilege(Connection conn, String schemaName, Consumer<TestItem> consumer) {
        String probeTable = "tap___test_" + UUID.randomUUID().toString().replace("-", "");
        String qualifiedTable = quoteIdentifier(schemaName) + "." + quoteIdentifier(probeTable);
        boolean created = false;
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE " + qualifiedTable
                    + " (id BIGINT PRIMARY KEY, probe_value VARCHAR)");
            created = true;
            st.executeUpdate("INSERT INTO " + qualifiedTable + " VALUES (1, 'initial')");
            st.execute("FLUSH");
            st.executeUpdate("UPDATE " + qualifiedTable + " SET probe_value = 'updated' WHERE id = 1");
            st.execute("FLUSH");
            try (ResultSet resultSet = st.executeQuery(
                    "SELECT probe_value FROM " + qualifiedTable + " WHERE id = 1")) {
                if (!resultSet.next() || !"updated".equals(resultSet.getString(1))) {
                    throw new SQLException("Updated probe row was not query-visible after FLUSH");
                }
            }
            st.executeUpdate("DELETE FROM " + qualifiedTable + " WHERE id = 1");
            st.execute("FLUSH");
            try (ResultSet resultSet = st.executeQuery(
                    "SELECT count(*) FROM " + qualifiedTable)) {
                if (!resultSet.next() || resultSet.getLong(1) != 0) {
                    throw new SQLException("Deleted probe row remained query-visible after FLUSH");
                }
            }
            st.execute("DROP TABLE " + qualifiedTable);
            created = false;
            consumer.accept(new TestItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY,
                    "Create, insert, update, delete, and drop succeeded in schema \"" + schemaName + "\""));
        } catch (Exception e) {
            debugLog("connectionTest() write privilege ERROR: " + e.getClass().getName() + ": " + e.getMessage());
            consumer.accept(new TestItem(TestItem.ITEM_WRITE, TestItem.RESULT_FAILED,
                    "Write privilege check failed in schema \"" + schemaName + "\": " + e.getMessage()));
        } finally {
            if (created) {
                try (Statement cleanup = conn.createStatement()) {
                    cleanup.execute("DROP TABLE IF EXISTS " + qualifiedTable);
                } catch (Exception cleanupError) {
                    debugLog("connectionTest() write probe cleanup ERROR table=" + qualifiedTable
                            + ": " + cleanupError.getMessage());
                }
            }
        }
    }

    private void testStreamingWrite(Connection conn, DataMap cfg, String schemaName,
                                    Consumer<TestItem> consumer, boolean jsonbMode) {
        String probeTable = "tap___test_" + UUID.randomUUID().toString().replace("-", "");
        String qualifiedTable = quoteIdentifier(schemaName) + "." + quoteIdentifier(probeTable);
        String ingestEndpoint = resolveIngestEndpoint(
                cfg.getString("ingestEndpoint"), cfg.getString("host"));
        String database = cfg.getString("database");
        if (database == null || database.isEmpty()) {
            database = "dev";
        }
        String webhookSecret = cfg.getString("webhookSecret");

        boolean created = false;
        Exception ddlError = null;
        Exception ingestError = null;
        try (Statement st = conn.createStatement()) {
            String columns = jsonbMode
                    ? "(" + quoteIdentifier(JSONB_PAYLOAD_COLUMN) + " JSONB)"
                    : "(id BIGINT, probe_value VARCHAR, PRIMARY KEY (id))";
            st.execute("CREATE TABLE " + qualifiedTable + " " + columns
                    + " WITH (connector = 'webhook')"
                    + webhookValidationClause(webhookSecret, jsonbMode));
            created = true;

            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", 1L);
            row.put("probe_value", "websocket_precheck");
            try (WsIngestClient client = new WsIngestClient(
                    ingestEndpoint, database, schemaName, probeTable, webhookSecret)) {
                client.connect();
                List<CompletableFuture<Void>> futures = client.sendBatch(Collections.singletonList(
                        new WsIngestClient.DmlOperation("insert", null, row)));
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                        .get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                ingestError = e;
                debugLog("connectionTest() websocket ingest ERROR endpoint=" + ingestEndpoint
                        + ": " + e.getClass().getName() + ": " + e.getMessage());
            }
        } catch (Exception e) {
            ddlError = e;
            debugLog("connectionTest() streaming DDL ERROR table=" + qualifiedTable
                    + ": " + e.getClass().getName() + ": " + e.getMessage());
        } finally {
            if (created) {
                try (Statement cleanup = conn.createStatement()) {
                    cleanup.execute("DROP TABLE " + qualifiedTable);
                } catch (Exception cleanupError) {
                    ddlError = cleanupError;
                    debugLog("connectionTest() streaming probe cleanup ERROR table=" + qualifiedTable
                            + ": " + cleanupError.getMessage());
                }
            }
        }

        if (ddlError == null) {
            consumer.accept(new TestItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY,
                    "Create and drop of a webhook-backed table succeeded in schema \""
                            + schemaName + "\""));
        } else {
            consumer.accept(new TestItem(TestItem.ITEM_WRITE, TestItem.RESULT_FAILED,
                    "Webhook table create/drop check failed in schema \"" + schemaName
                            + "\": " + ddlError.getMessage()));
        }

        if (!created) {
            consumer.accept(new TestItem(INGEST_ENDPOINT_TEST_ITEM, TestItem.RESULT_FAILED,
                    "WebSocket ingest cannot be verified because the temporary webhook table could not be created"));
        } else if (ingestError == null) {
            consumer.accept(new TestItem(INGEST_ENDPOINT_TEST_ITEM, TestItem.RESULT_SUCCESSFULLY,
                    "WebSocket connection, signed init, DML write, and RisingWave ACK succeeded"));
        } else {
            consumer.accept(new TestItem(INGEST_ENDPOINT_TEST_ITEM, TestItem.RESULT_FAILED,
                    "WebSocket ingest check failed: " + ingestError.getMessage()));
        }
    }

    static boolean supportsWebSocketIngest(String versionOutput) {
        int[] version = parseRisingWaveVersion(versionOutput);
        if (version == null) {
            return false;
        }
        return version[0] > 3 || (version[0] == 3 && version[1] >= 0);
    }

    static int[] parseRisingWaveVersion(String versionOutput) {
        if (versionOutput == null) {
            return null;
        }
        java.util.regex.Matcher matcher = RISINGWAVE_VERSION_PATTERN.matcher(versionOutput);
        if (!matcher.find()) {
            return null;
        }
        int patch = matcher.group(3) == null ? 0 : Integer.parseInt(matcher.group(3));
        return new int[]{
                Integer.parseInt(matcher.group(1)),
                Integer.parseInt(matcher.group(2)),
                patch
        };
    }

    @Override
    public void discoverSchema(TapConnectionContext context, List<String> tables,
                               int tableSize,
                               Consumer<List<TapTable>> consumer) throws Throwable {
        String schemaName = getSchema(context);
        String tableFilter = buildTableFilter(tables);
        debugLog("discoverSchema() start schema=" + schemaName + " tables=" + tables
                + " tableSize=" + tableSize);

        // Use information_schema which is supported by RisingWave
        String colSql = "SELECT table_name, column_name, data_type, udt_name, " +
                "is_nullable, character_maximum_length, numeric_precision, numeric_scale, ordinal_position " +
                "FROM information_schema.columns " +
                "WHERE table_schema = ?" +
                (tableFilter.isEmpty() ? "" : " AND table_name IN (" + tableFilter + ")") +
                " ORDER BY table_name, ordinal_position";

        // Also get primary key info
        String pkSql = "SELECT tc.table_name, kcu.column_name " +
                "FROM information_schema.table_constraints tc " +
                "JOIN information_schema.key_column_usage kcu " +
                "  ON tc.constraint_name = kcu.constraint_name " +
                "  AND tc.table_schema = kcu.table_schema " +
                "WHERE tc.constraint_type = 'PRIMARY KEY' " +
                "  AND tc.table_schema = ? " +
                (tableFilter.isEmpty() ? "" : " AND tc.table_name IN (" + tableFilter + ")") +
                " ORDER BY tc.table_name, kcu.ordinal_position";

        Map<String, List<String>> pkMap = new LinkedHashMap<>();
        try (PreparedStatement ps = connection.prepareStatement(pkSql)) {
            ps.setString(1, schemaName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    pkMap.computeIfAbsent(rs.getString("table_name"),
                            k -> new ArrayList<>()).add(rs.getString("column_name"));
                }
            }
        } catch (Exception e) {
            debugLog("discoverSchema() PK discovery ERROR: " + e.getClass().getName() + ": " + e.getMessage());
            // PK discovery is best-effort
        }

        Map<String, TapTable> tapTables = new LinkedHashMap<>();
        try (PreparedStatement ps = connection.prepareStatement(colSql)) {
            ps.setString(1, schemaName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String tableName = rs.getString("table_name");
                    TapTable tapTable = tapTables.computeIfAbsent(tableName, TapTable::new);
                    TapField field = new TapField();
                    field.setName(rs.getString("column_name"));
                    field.setDataType(mapDataType(rs.getString("data_type"),
                            rs.getString("udt_name"),
                            rs.getObject("character_maximum_length"),
                            rs.getObject("numeric_precision"),
                            rs.getObject("numeric_scale")));
                    field.setNullable("YES".equalsIgnoreCase(rs.getString("is_nullable")));
                    tapTable.add(field);
                }
            }
        }

        // Assign primary keys
        for (Map.Entry<String, TapTable> entry : tapTables.entrySet()) {
            List<String> pks = pkMap.get(entry.getKey());
            if (pks != null && !pks.isEmpty()) {
                entry.getValue().defaultPrimaryKeys(pks);
            }
        }

        // Discover table list if no tables specified and columns query returned nothing
        if (tapTables.isEmpty()) {
            String tablesSql = "SELECT table_name FROM information_schema.tables " +
                    "WHERE table_schema = ? AND table_type = 'BASE TABLE' " +
                    "ORDER BY table_name";
            try (PreparedStatement ps = connection.prepareStatement(tablesSql)) {
                ps.setString(1, schemaName);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        tapTables.put(rs.getString("table_name"),
                                new TapTable(rs.getString("table_name")));
                    }
                }
            }
        }

        debugLog("discoverSchema() discovered tables=" + tapTables.keySet());
        // Stream results in batches
        List<TapTable> batch = new ArrayList<>();
        for (TapTable table : tapTables.values()) {
            batch.add(table);
            if (batch.size() >= tableSize) {
                consumer.accept(batch);
                batch = new ArrayList<>();
            }
        }
        if (!batch.isEmpty()) {
            consumer.accept(batch);
        }
    }

    @Override
    public int tableCount(TapConnectionContext context) throws Throwable {
        String schemaName = getSchema(context);
        String sql = "SELECT count(*) FROM information_schema.tables " +
                "WHERE table_schema = ? AND table_type = 'BASE TABLE'";
        debugLog("tableCount() schema=" + schemaName);
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            try (ResultSet rs = ps.executeQuery()) {
                int count = rs.next() ? rs.getInt(1) : 0;
                debugLog("tableCount() result=" + count);
                return count;
            }
        }
    }

    // ---- TapConnector ----

    @Override
    public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry registry) {
        functions.supportWriteRecord(this::writeRecord);
        functions.supportCreateTableV2(this::createTable);
        functions.supportClearTable(this::clearTable);
        functions.supportDropTable(this::dropTable);
        functions.supportReleaseExternalFunction(this::releaseExternal);

        registry.registerFromTapValue(TapRawValue.class, "text", value ->
                value == null || value.getValue() == null ? null : TapSimplify.toJson(value.getValue()));
        registry.registerFromTapValue(TapMapValue.class, "jsonb", value ->
                value == null || value.getValue() == null ? null : TapSimplify.toJson(value.getValue()));
        registry.registerFromTapValue(TapArrayValue.class, "jsonb", value ->
                value == null || value.getValue() == null ? null : TapSimplify.toJson(value.getValue()));
        registry.registerFromTapValue(TapJsonValue.class, "jsonb", value ->
                value == null ? null : value.getValue());
        registry.registerFromTapValue(TapBinaryValue.class, "bytea", value ->
                value == null || value.getValue() == null ? null : value.getValue().getValue());
        registry.registerFromTapValue(TapTimeValue.class, value ->
                value == null || value.getValue() == null ? null : value.getValue().toTime());
        registry.registerFromTapValue(TapDateTimeValue.class, value ->
                value == null || value.getValue() == null ? null : value.getValue().toTimestamp());
        registry.registerFromTapValue(TapDateValue.class, value ->
                value == null || value.getValue() == null ? null : value.getValue().toSqlDate());
    }

    // ---- Write Record ----

    private void writeRecord(TapConnectorContext context,
                             List<TapRecordEvent> events,
                             TapTable table,
                             Consumer<WriteListResult<TapRecordEvent>> resultConsumer) throws Throwable {
        debugLog("writeRecord() mode=" + ingestMode + " tableId="
                + (table == null ? "null" : table.getId()) + " events=" + events.size());
        if (MODE_STREAMING.equals(ingestMode)) {
            writeRecordStreaming(events, table, resultConsumer);
        } else if (MODE_STREAMING_JSONB.equals(ingestMode)) {
            writeRecordStreamingJsonb(events, table, resultConsumer);
        } else {
            writeRecordJdbc(events, table, resultConsumer);
        }
    }

    // ---- Streaming (WebSocket) write path ----

    private void writeRecordStreaming(List<TapRecordEvent> events,
                                       TapTable table,
                                       Consumer<WriteListResult<TapRecordEvent>> resultConsumer) throws Throwable {
        validateStreamingPrimaryKey(table);
        WriteListResult<TapRecordEvent> result = new WriteListResult<>();
        long inserted = 0, updated = 0, deleted = 0;

        debugLog("writeRecordStreaming start table=" + table.getId()
                + " events=" + events.size()
                + " pk=" + primaryKeysOf(table));
        WsIngestClient client = getOrCreateWsClient(table.getId());
        List<WsIngestClient.DmlOperation> operations = new ArrayList<>(events.size());

        for (TapRecordEvent event : events) {
            if (!alive.get()) {
                throw new java.util.concurrent.CancellationException("Connector is stopping");
            }
            try {
                if (event instanceof TapInsertRecordEvent) {
                    TapInsertRecordEvent insert = (TapInsertRecordEvent) event;
                    Map<String, Object> after = normalizeRecordForStreaming(insert.getAfter(), table, true);
                    operations.add(new WsIngestClient.DmlOperation("insert", null, after));
                    inserted++;
                } else if (event instanceof TapUpdateRecordEvent) {
                    TapUpdateRecordEvent update = (TapUpdateRecordEvent) event;
                    Map<String, Object> before = normalizeRecordForStreaming(update.getBefore(), table, true);
                    Map<String, Object> after = normalizeRecordForStreaming(update.getAfter(), table, true);
                    if (requiresDeleteBeforeUpsert(table, before, after)) {
                        operations.add(new WsIngestClient.DmlOperation("delete", before, null));
                    }
                    operations.add(new WsIngestClient.DmlOperation("update", before, after));
                    updated++;
                } else if (event instanceof TapDeleteRecordEvent) {
                    TapDeleteRecordEvent delete = (TapDeleteRecordEvent) event;
                    Map<String, Object> before = normalizeRecordForStreaming(delete.getBefore(), table, true);
                    operations.add(new WsIngestClient.DmlOperation("delete", before, null));
                    deleted++;
                } else {
                    debugLog("writeRecordStreaming skip event type=" + event.getClass().getName());
                    continue;
                }
            } catch (Exception e) {
                debugLog("writeRecordStreaming send ERROR: " + e.getMessage());
                result.addError(event, e);
            }
        }

        List<CompletableFuture<Void>> futures = client.sendBatch(operations);

        // Wait for all DMLs in this batch to be acked (persisted to Hummock).
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(120, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception e) {
            debugLog("writeRecordStreaming ack wait ERROR: " + e.getMessage());
            // On ack failure, reconnect the client so next batch starts fresh.
            removeWsClient(table.getId());
            throw new RuntimeException("Streaming ingest ack failed: " + e.getMessage(), e);
        }

        debugLog("writeRecordStreaming done: inserted=" + inserted + " updated=" + updated + " deleted=" + deleted);
        resultConsumer.accept(result.insertedCount(inserted)
                .modifiedCount(updated)
                .removedCount(deleted));
    }

    private void writeRecordStreamingJsonb(List<TapRecordEvent> events,
                                            TapTable table,
                                            Consumer<WriteListResult<TapRecordEvent>> resultConsumer) throws Throwable {
        validateJsonbAppendOnlyEvents(events, table);
        if (events.isEmpty()) {
            resultConsumer.accept(new WriteListResult<TapRecordEvent>().insertedCount(0));
            return;
        }

        WsIngestClient client = getOrCreateWsClient(table.getId());
        List<WsIngestClient.DmlOperation> operations = new ArrayList<>(events.size());
        for (TapRecordEvent event : events) {
            if (!alive.get()) {
                throw new java.util.concurrent.CancellationException("Connector is stopping");
            }
            TapInsertRecordEvent insert = (TapInsertRecordEvent) event;
            Map<String, Object> document = normalizeRecordForStreaming(insert.getAfter(), table, false);
            operations.add(new WsIngestClient.DmlOperation("insert", null, document));
        }

        try {
            List<CompletableFuture<Void>> futures = client.sendBatch(operations);
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(120, TimeUnit.SECONDS);
        } catch (Exception e) {
            removeWsClient(table.getId());
            throw new RuntimeException("JSONB append-only ingest ack failed: " + e.getMessage(), e);
        }

        resultConsumer.accept(new WriteListResult<TapRecordEvent>().insertedCount(events.size()));
    }

    static void validateJsonbAppendOnlyEvents(List<TapRecordEvent> events, TapTable table) {
        String tableName = table == null ? "<unknown>" : table.getId();
        for (TapRecordEvent event : events) {
            if (!(event instanceof TapInsertRecordEvent)) {
                throw new IllegalArgumentException("WebSocket JSONB append-only mode accepts only inserts for table "
                        + tableName + "; received " + event.getClass().getSimpleName());
            }
        }
    }

    static boolean requiresDeleteBeforeUpsert(TapTable table,
                                              Map<String, Object> before,
                                              Map<String, Object> after) {
        if (before == null || after == null) {
            return false;
        }
        Collection<String> primaryKeys = primaryKeysOf(table);
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            // Follow the database connector fallback: without a declared key, use the full
            // before image to retract the old row before inserting the new image.
            return !before.equals(after);
        }
        for (String primaryKey : primaryKeys) {
            if (!Objects.equals(before.get(primaryKey), after.get(primaryKey))) {
                return true;
            }
        }
        return false;
    }

    /** Get or create (and connect) a WsIngestClient for the given table. */
    private synchronized WsIngestClient getOrCreateWsClient(String tableId) throws Exception {
        WsIngestClient client = wsClients.get(tableId);
        if (client == null) {
            // Extract just the table name without schema prefix, if present
            String tableName = tableId.contains(".") ? tableId.split("\\.", 2)[1] : tableId;
            client = new WsIngestClient(wsIngestEndpoint, wsDatabase, schema, tableName, wsWebhookSecret);
            client.connect();
            wsClients.put(tableId, client);
            debugLog("Created WsIngestClient for table=" + tableId + " ingestEndpoint=" + wsIngestEndpoint);
        }
        return client;
    }

    /** Remove and close the WsIngestClient for a table (called on error to force reconnect). */
    private synchronized void removeWsClient(String tableId) {
        WsIngestClient client = wsClients.remove(tableId);
        if (client != null) {
            try { client.close(); } catch (Exception ignored) {}
        }
    }

    // ---- JDBC write path (original) ----

    private void writeRecordJdbc(List<TapRecordEvent> events,
                                   TapTable table,
                                   Consumer<WriteListResult<TapRecordEvent>> resultConsumer) throws Throwable {
        WriteListResult<TapRecordEvent> result = new WriteListResult<>();
        long inserted = 0, updated = 0, deleted = 0;
        boolean dirty = false;
        Set<List<Object>> pendingInsertIdentities = new HashSet<>();

        debugLog("writeRecord called: " + events.size() + " events for table " + table.getId());
        // RisingWave SQL DML can become visible asynchronously. Establish a visibility boundary
        // before applying this batch so retries and update/delete lookups observe prior writes.
        flushJdbcWrites();

        for (TapRecordEvent event : events) {
            try {
                if (event instanceof TapInsertRecordEvent) {
                    TapInsertRecordEvent insert = (TapInsertRecordEvent) event;
                    List<Object> identity = keyedIdentityOf(table, insert.getAfter());
                    if (dirty && identity != null && pendingInsertIdentities.contains(identity)) {
                        // RisingWave SQL DML is not guaranteed to be query-visible until FLUSH.
                        // Make an earlier insert with the same key visible before manual upsert.
                        flushJdbcWrites();
                        dirty = false;
                        pendingInsertIdentities.clear();
                    }
                    // Keyed inserts use upsert so replay after a transport/FLUSH failure is idempotent.
                    doInsertOrUpdate(table, insert.getAfter());
                    dirty = true;
                    if (identity != null) {
                        pendingInsertIdentities.add(identity);
                    }
                    inserted++;
                } else if (event instanceof TapUpdateRecordEvent) {
                    if (dirty) {
                        flushJdbcWrites();
                        dirty = false;
                        pendingInsertIdentities.clear();
                    }
                    TapUpdateRecordEvent update = (TapUpdateRecordEvent) event;
                    if (requiresDeleteBeforeUpsert(table, update.getBefore(), update.getAfter())) {
                        doDelete(table, update.getBefore());
                        doInsertOrUpdate(table, update.getAfter());
                    } else {
                        int rows = doUpdate(table, update.getBefore(), update.getAfter());
                        if (rows == 0) {
                            // The before image no longer exists; apply the configured upsert fallback.
                            doInsertOrUpdate(table, update.getAfter());
                        }
                    }
                    dirty = true;
                    updated++;
                } else if (event instanceof TapDeleteRecordEvent) {
                    if (dirty) {
                        flushJdbcWrites();
                        dirty = false;
                        pendingInsertIdentities.clear();
                    }
                    TapDeleteRecordEvent delete = (TapDeleteRecordEvent) event;
                    doDelete(table, delete.getBefore());
                    dirty = true;
                    deleted++;
                }
            } catch (Exception e) {
                debugLog("writeRecord ERROR: " + e.getClass().getName() + ": " + e.getMessage());
                result.addError(event, e);
            }
        }

        if (dirty) {
            flushJdbcWrites();
        }

        debugLog("writeRecord done: inserted=" + inserted + " updated=" + updated + " deleted=" + deleted);
        resultConsumer.accept(result.insertedCount(inserted)
                .modifiedCount(updated)
                .removedCount(deleted));
    }

    private static List<Object> keyedIdentityOf(TapTable table, Map<String, Object> record) {
        Collection<String> primaryKeys = primaryKeysOf(table);
        if (record == null || primaryKeys == null || primaryKeys.isEmpty()
                || !record.keySet().containsAll(primaryKeys)) {
            return null;
        }
        List<Object> identity = new ArrayList<>(primaryKeys.size());
        for (String primaryKey : primaryKeys) {
            identity.add(record.get(primaryKey));
        }
        return identity;
    }

    private void doInsert(TapTable table, Map<String, Object> record) throws SQLException {
        if (record == null || record.isEmpty()) return;
        List<String> cols = new ArrayList<>(record.keySet());
        String tableName = fullTableName(table.getId());
        String sql = "INSERT INTO " + tableName + " (" +
                quoteCols(cols) + ") VALUES (" +
                placeholders(cols.size()) + ")";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (int i = 0; i < cols.size(); i++) {
                setParam(ps, i + 1, record.get(cols.get(i)));
            }
            ps.executeUpdate();
        }
    }

    private int doUpdate(TapTable table, Map<String, Object> before, Map<String, Object> after) throws SQLException {
        Collection<String> pks = primaryKeysOf(table);
        if (before == null || before.isEmpty() || after == null || after.isEmpty()) {
            return 0;
        }
        List<String> setCols = new ArrayList<>();
        for (String column : after.keySet()) {
            if (pks == null || !pks.contains(column)) {
                setCols.add(column);
            }
        }
        if (setCols.isEmpty()) return 1;
        List<String> filterCols = pks != null && !pks.isEmpty()
                ? new ArrayList<>(pks) : new ArrayList<>(before.keySet());
        if (filterCols.isEmpty()) return 0;

        String tableName = fullTableName(table.getId());
        StringBuilder sql = new StringBuilder("UPDATE ").append(tableName).append(" SET ");
        for (int i = 0; i < setCols.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append(quoteIdentifier(setCols.get(i))).append(" = ?");
        }
        sql.append(" WHERE ");
        for (int i = 0; i < filterCols.size(); i++) {
            if (i > 0) sql.append(" AND ");
            String column = filterCols.get(i);
            sql.append(quoteIdentifier(column));
            if (before.get(column) == null) {
                sql.append(" IS NULL");
            } else {
                sql.append(" = ?");
            }
        }

        try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            int idx = 1;
            for (String col : setCols) {
                setParam(ps, idx++, after.get(col));
            }
            for (String column : filterCols) {
                Object value = before.get(column);
                if (value != null) {
                    setParam(ps, idx++, value);
                }
            }
            return ps.executeUpdate();
        }
    }

    private void doInsertOrUpdate(TapTable table, Map<String, Object> record) throws SQLException {
        Collection<String> pks = primaryKeysOf(table);
        if (pks == null || pks.isEmpty() || record == null) {
            doInsert(table, record);
            return;
        }
        if (hasOnlyPrimaryKeyColumns(record, pks)) {
            if (!rowExistsByPrimaryKey(table, record, pks)) {
                doInsert(table, record);
            }
            return;
        }
        if (doUpdate(table, record, record) == 0) {
            doInsert(table, record);
        }
    }

    private static boolean hasOnlyPrimaryKeyColumns(
            Map<String, Object> record, Collection<String> primaryKeys) {
        return primaryKeys.containsAll(record.keySet());
    }

    private boolean rowExistsByPrimaryKey(
            TapTable table, Map<String, Object> record, Collection<String> primaryKeys) throws SQLException {
        List<String> columns = new ArrayList<>(primaryKeys);
        StringBuilder sql = new StringBuilder("SELECT 1 FROM ")
                .append(fullTableName(table.getId())).append(" WHERE ");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) sql.append(" AND ");
            String column = columns.get(i);
            sql.append(quoteIdentifier(column));
            if (record.get(column) == null) {
                sql.append(" IS NULL");
            } else {
                sql.append(" = ?");
            }
        }
        try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            int index = 1;
            for (String column : columns) {
                Object value = record.get(column);
                if (value != null) {
                    setParam(statement, index++, value);
                }
            }
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next();
            }
        }
    }

    private void doDelete(TapTable table, Map<String, Object> record) throws SQLException {
        Collection<String> pks = primaryKeysOf(table);
        if (record == null) return;

        String tableName = fullTableName(table.getId());
        // Use PKs if available, otherwise use all columns as filter
        List<String> filterCols = (pks != null && !pks.isEmpty())
                ? new ArrayList<>(pks) : new ArrayList<>(record.keySet());

        StringBuilder sql = new StringBuilder("DELETE FROM ").append(tableName).append(" WHERE ");
        for (int i = 0; i < filterCols.size(); i++) {
            if (i > 0) sql.append(" AND ");
            sql.append(quoteIdentifier(filterCols.get(i)));
            if (record.get(filterCols.get(i)) == null) {
                sql.append(" IS NULL");
            } else {
                sql.append(" = ?");
            }
        }

        try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            int idx = 1;
            for (String col : filterCols) {
                Object val = record.get(col);
                if (val != null) setParam(ps, idx++, val);
            }
            ps.executeUpdate();
        }
    }

    private void flushJdbcWrites() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("FLUSH");
        }
    }

    // ---- DDL ----

    io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions createTable(
            TapConnectorContext context,
            io.tapdata.entity.event.ddl.table.TapCreateTableEvent event) throws Throwable {
        io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions opts =
                io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions.create();
        TapTable table = event.getTable();
        if (table == null) return opts;

        if (MODE_STREAMING.equals(ingestMode)) {
            validateStreamingPrimaryKey(table);
        }

        LinkedHashMap<String, TapField> fields = table.getNameFieldMap();
        if (fields == null || fields.isEmpty()) {
            debugLog("createTable() empty fields table=" + table.getId());
            return opts;
        }

        TableReference tableReference = tableReference(table.getId());
        String tableName = tableReference.qualifiedName();
        if (tableExists(connection, tableReference)) {
            if (MODE_STREAMING_JSONB.equals(ingestMode)) {
                validateExistingJsonbTable(connection, tableReference);
            } else {
                validateExistingTable(connection, tableReference, table);
            }
            opts.tableExists(true);
            return opts;
        }
        StringBuilder sql = new StringBuilder("CREATE TABLE ").append(tableName).append(" (");

        if (MODE_STREAMING_JSONB.equals(ingestMode)) {
            sql.append(quoteIdentifier(JSONB_PAYLOAD_COLUMN)).append(" JSONB)")
                    .append(" WITH (connector = 'webhook')")
                    .append(webhookValidationClause(wsWebhookSecret, true));
            executeCreateTable(sql.toString(), table.getId(), opts);
            return opts;
        }

        Collection<String> pks = primaryKeysOf(table);
        List<String> colDefs = new ArrayList<>();
        for (Map.Entry<String, TapField> entry : fields.entrySet()) {
            TapField f = entry.getValue();
            String colDef = quoteIdentifier(f.getName()) + " " + toRisingWaveType(f);
            if (!MODE_STREAMING.equals(ingestMode) && Boolean.FALSE.equals(f.getNullable())) {
                colDef += " NOT NULL";
            }
            colDefs.add(colDef);
        }
        sql.append(String.join(", ", colDefs));

        if (pks != null && !pks.isEmpty()) {
            sql.append(", PRIMARY KEY (").append(quoteCols(new ArrayList<>(pks))).append(")");
        }
        sql.append(")");
        if (MODE_STREAMING.equals(ingestMode)) {
            sql.append(" WITH (connector = 'webhook')");
            sql.append(webhookValidationClause(wsWebhookSecret, false));
        }

        executeCreateTable(sql.toString(), table.getId(), opts);
        return opts;
    }

    private void executeCreateTable(String sql, String tableId,
                                    io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions opts)
            throws SQLException {
        debugLog("createTable() table=" + tableId + " mode=" + ingestMode);
        try (Statement st = connection.createStatement()) {
            st.execute(sql);
            debugLog("createTable() success table=" + tableId);
        } catch (Exception e) {
            debugLog("createTable() ERROR table=" + tableId + ": "
                    + e.getClass().getName() + ": " + e.getMessage());
            if (e instanceof SQLException) {
                throw (SQLException) e;
            }
            throw new SQLException("Failed to create target table " + tableId, e);
        }
        opts.tableExists(false);
    }

    private void clearTable(TapConnectorContext context,
                            io.tapdata.entity.event.ddl.table.TapClearTableEvent event) throws Throwable {
        String tableName = fullTableName(event.getTableId());
        try (Statement st = connection.createStatement()) {
            st.execute("DELETE FROM " + tableName);
        }
    }

    private void dropTable(TapConnectorContext context,
                           io.tapdata.entity.event.ddl.table.TapDropTableEvent event) throws Throwable {
        removeWsClient(event.getTableId());
        String tableName = fullTableName(event.getTableId());
        try (Statement st = connection.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + tableName);
        }
    }

    // ---- Helpers ----

    private Connection openConnection(TapConnectionContext context) throws SQLException {
        // Explicitly load the JDBC driver - required when running inside Tapdata's PDK classloader
        // where ServiceLoader-based auto-discovery may not work for shaded JARs
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new SQLException("PostgreSQL JDBC driver not found in classpath", e);
        }
        DataMap cfg = context.getConnectionConfig();
        String host = cfg.getString("host");
        Object portObj = cfg.getObject("port");
        int port = portObj != null ? Integer.parseInt(portObj.toString()) : 4566;
        String database = cfg.getString("database");
        if (database == null || database.isEmpty()) database = "dev";
        String user = cfg.getString("user");
        if (user == null || user.isEmpty()) user = "root";
        String password = cfg.getString("password");
        if (password == null) password = "";

        String url = "jdbc:postgresql://" + host + ":" + port + "/" + database +
                "?socketTimeout=30&loginTimeout=30&tcpKeepAlive=true";
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        // Use a driver property so offsets such as +08:00 are not corrupted by URL decoding.
        String timezone = cfg.getString("timezone");
        if (timezone != null && !timezone.isEmpty()) {
            props.setProperty("options", "-c timezone=" + timezone);
        }
        // SSL mode: prefer by default (works for both local non-TLS and cloud TLS deployments).
        // Cloud deployments require TLS for SNI-based tenant routing.
        String sslmode = cfg.getString("sslmode");
        if (sslmode == null || sslmode.isEmpty()) sslmode = "prefer";
        props.setProperty("sslmode", sslmode);
        // Extra JDBC parameters
        String extParams = cfg.getString("extParams");
        if (extParams != null && !extParams.isEmpty()) {
            url += "&" + extParams;
        }
        return DriverManager.getConnection(url, props);
    }

    private String getSchema(TapConnectionContext context) {
        if (schema != null && !schema.isEmpty()) return schema;
        return configuredSchema(context.getConnectionConfig());
    }

    private static String configuredSchema(DataMap cfg) {
        String configuredSchema = cfg.getString("schema");
        return (configuredSchema != null && !configuredSchema.isEmpty()) ? configuredSchema : "public";
    }

    static String resolveIngestEndpoint(String configuredEndpoint, String host) {
        String endpoint = configuredEndpoint;
        if (endpoint == null || endpoint.trim().isEmpty()) {
            endpoint = "ws://{Host}:4560";
        }
        if (endpoint.contains("{Host}")) {
            if (host == null || host.trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "Host is required to resolve the default WebSocket ingest endpoint");
            }
            endpoint = endpoint.replace("{Host}", host.trim());
        }
        return endpoint;
    }

    static String quoteIdentifier(String identifier) {
        Objects.requireNonNull(identifier, "identifier");
        return '"' + identifier.replace("\"", "\"\"") + '"';
    }

    private static String webhookValidationClause(String webhookSecret, boolean jsonbMode) {
        if (webhookSecret == null || webhookSecret.isEmpty()) {
            return "";
        }
        String escapedSecret = webhookSecret.replace("'", "''");
        String signedPayload = jsonbMode ? quoteIdentifier(JSONB_PAYLOAD_COLUMN) : "payload";
        return " VALIDATE AS secure_compare("
                + "headers->>'x-rw-signature', "
                + "'sha256=' || encode(hmac('" + escapedSecret
                + "', " + signedPayload + ", 'sha256'), 'hex'))";
    }

    static boolean isWebSocketMode(String mode) {
        return MODE_STREAMING.equals(mode) || MODE_STREAMING_JSONB.equals(mode);
    }

    private String fullTableName(String tableId) {
        return tableReference(tableId).qualifiedName();
    }

    private TableReference tableReference(String tableId) {
        Objects.requireNonNull(tableId, "tableId");
        int separator = tableId.indexOf('.');
        if (separator > 0 && separator < tableId.length() - 1) {
            return new TableReference(tableId.substring(0, separator), tableId.substring(separator + 1));
        }
        return new TableReference(schema, tableId);
    }

    private boolean tableExists(Connection conn, TableReference table) throws SQLException {
        String sql = "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, table.schema);
            statement.setString(2, table.table);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next();
            }
        }
    }

    private void validateExistingTable(Connection conn, TableReference table,
                                       TapTable expectedTable) throws SQLException {
        Map<String, String> existingColumns = new LinkedHashMap<>();
        String columnsSql = "SELECT column_name, data_type FROM information_schema.columns "
                + "WHERE table_schema = ? AND table_name = ?";
        try (PreparedStatement statement = conn.prepareStatement(columnsSql)) {
            statement.setString(1, table.schema);
            statement.setString(2, table.table);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    existingColumns.put(resultSet.getString(1), resultSet.getString(2));
                }
            }
        }
        List<String> missingColumns = new ArrayList<>();
        List<String> incompatibleColumns = new ArrayList<>();
        for (Map.Entry<String, TapField> expected : expectedTable.getNameFieldMap().entrySet()) {
            String requiredColumn = expected.getKey();
            if (!existingColumns.containsKey(requiredColumn)) {
                missingColumns.add(requiredColumn);
                continue;
            }
            String expectedType = canonicalRisingWaveType(toRisingWaveType(expected.getValue()));
            String actualType = canonicalRisingWaveType(existingColumns.get(requiredColumn));
            if (!expectedType.equals(actualType)) {
                incompatibleColumns.add(requiredColumn + " (expected " + expectedType
                        + ", found " + actualType + ")");
            }
        }
        if (!missingColumns.isEmpty()) {
            throw new SQLException("Existing target table " + table.qualifiedName()
                    + " is missing columns required by the Tapdata model: " + missingColumns);
        }
        if (!incompatibleColumns.isEmpty()) {
            throw new SQLException("Existing target table " + table.qualifiedName()
                    + " has incompatible column types: " + incompatibleColumns);
        }

        Set<String> expectedPrimaryKeys = new LinkedHashSet<>();
        expectedPrimaryKeys.addAll(primaryKeysOf(expectedTable));
        Set<String> existingPrimaryKeys = loadPrimaryKeys(conn, table);
        if (!expectedPrimaryKeys.equals(existingPrimaryKeys)) {
            throw new SQLException("Existing target table " + table.qualifiedName()
                    + " has primary key " + existingPrimaryKeys
                    + " but the Tapdata model requires " + expectedPrimaryKeys);
        }

        if (isWebSocketMode(ingestMode)) {
            try (Statement statement = conn.createStatement();
                 ResultSet resultSet = statement.executeQuery("SHOW CREATE TABLE " + table.qualifiedName())) {
                String ddl = resultSet.next() ? resultSet.getString(2) : "";
                String normalizedDdl = ddl == null ? "" : ddl.toLowerCase(Locale.ROOT);
                if (!normalizedDdl.contains("connector = 'webhook'")
                        && !normalizedDdl.contains("connector='webhook'")) {
                    throw new SQLException("Existing target table " + table.qualifiedName()
                            + " is not webhook-backed and cannot receive WebSocket streaming writes");
                }
            }
        }
    }

    private void validateExistingJsonbTable(Connection conn, TableReference table) throws SQLException {
        Map<String, String> columns = new LinkedHashMap<>();
        String sql = "SELECT column_name, data_type FROM information_schema.columns "
                + "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, table.schema);
            statement.setString(2, table.table);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    columns.put(resultSet.getString(1), resultSet.getString(2));
                }
            }
        }
        if (columns.size() != 1
                || !"jsonb".equals(canonicalRisingWaveType(columns.get(JSONB_PAYLOAD_COLUMN)))) {
            throw new SQLException("Existing target table " + table.qualifiedName()
                    + " must contain exactly one JSONB column named \"" + JSONB_PAYLOAD_COLUMN
                    + "\" for WebSocket JSONB append-only mode; found " + columns);
        }
        if (!loadPrimaryKeys(conn, table).isEmpty()) {
            throw new SQLException("Existing target table " + table.qualifiedName()
                    + " must not have a primary key in WebSocket JSONB append-only mode");
        }
        validateWebhookBackedTable(conn, table);
    }

    private void validateWebhookBackedTable(Connection conn, TableReference table) throws SQLException {
        try (Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery("SHOW CREATE TABLE " + table.qualifiedName())) {
            String ddl = resultSet.next() ? resultSet.getString(2) : "";
            String normalizedDdl = ddl == null ? "" : ddl.toLowerCase(Locale.ROOT);
            if (!normalizedDdl.contains("connector = 'webhook'")
                    && !normalizedDdl.contains("connector='webhook'")) {
                throw new SQLException("Existing target table " + table.qualifiedName()
                        + " is not webhook-backed and cannot receive WebSocket streaming writes");
            }
        }
    }

    private Set<String> loadPrimaryKeys(Connection conn, TableReference table) throws SQLException {
        Set<String> primaryKeys = new LinkedHashSet<>();
        String sql = "SELECT kcu.column_name FROM information_schema.table_constraints tc "
                + "JOIN information_schema.key_column_usage kcu "
                + "ON tc.constraint_catalog = kcu.constraint_catalog "
                + "AND tc.constraint_schema = kcu.constraint_schema "
                + "AND tc.constraint_name = kcu.constraint_name "
                + "WHERE tc.constraint_type = 'PRIMARY KEY' "
                + "AND tc.table_schema = ? AND tc.table_name = ? "
                + "ORDER BY kcu.ordinal_position";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, table.schema);
            statement.setString(2, table.table);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    primaryKeys.add(resultSet.getString(1));
                }
            }
        }
        return primaryKeys;
    }

    static void validateStreamingPrimaryKey(TapTable table) {
        Collection<String> primaryKeys = primaryKeysOf(table);
        if (primaryKeys.isEmpty()) {
            String tableName = table == null ? "<unknown>" : table.getId();
            throw new IllegalArgumentException("WebSocket streaming requires a primary key for table "
                    + tableName + " so updates, deletes, and retries preserve row identity");
        }
    }

    private static List<String> primaryKeysOf(TapTable table) {
        if (table == null || table.getNameFieldMap() == null) {
            return Collections.emptyList();
        }
        List<TapField> fields = new ArrayList<>();
        for (TapField field : table.getNameFieldMap().values()) {
            if (Boolean.TRUE.equals(field.getPrimaryKey())) {
                fields.add(field);
            }
        }
        fields.sort(Comparator.comparing(
                TapField::getPrimaryKeyPos,
                Comparator.nullsLast(Comparator.naturalOrder())));
        List<String> primaryKeys = new ArrayList<>(fields.size());
        for (TapField field : fields) {
            primaryKeys.add(field.getName());
        }
        return primaryKeys;
    }

    static String canonicalRisingWaveType(String dataType) {
        if (dataType == null) {
            return "text";
        }
        String type = dataType.trim().toLowerCase(Locale.ROOT).replaceAll("\\s+", " ");
        int parameters = type.indexOf('(');
        if (parameters >= 0) {
            type = type.substring(0, parameters).trim();
        }
        switch (type) {
            case "int": case "int4": case "integer": return "integer";
            case "int8": case "bigint": case "bigserial": case "serial": return "bigint";
            case "int2": case "smallint": return "smallint";
            case "float4": case "real": return "real";
            case "float8": case "double precision": return "double precision";
            case "decimal": case "numeric": return "numeric";
            case "bool": case "boolean": return "boolean";
            case "char": case "character": case "character varying": case "text":
            case "varchar": case "uuid": return "varchar";
            case "time without time zone": case "time": return "time";
            case "timestamp without time zone": case "timestamp": return "timestamp";
            case "timestamptz": case "timestamp with time zone": return "timestamp with time zone";
            case "json": case "jsonb": return "jsonb";
            default: return type;
        }
    }

    private static final class TableReference {
        private final String schema;
        private final String table;

        private TableReference(String schema, String table) {
            this.schema = Objects.requireNonNull(schema, "schema");
            this.table = Objects.requireNonNull(table, "table");
        }

        private String qualifiedName() {
            return quoteIdentifier(schema) + "." + quoteIdentifier(table);
        }
    }

    private static String buildTableFilter(List<String> tables) {
        if (tables == null || tables.isEmpty()) return "";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < tables.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append("'").append(tables.get(i).replace("'", "''")).append("'");
        }
        return sb.toString();
    }

    private static String quoteCols(List<String> cols) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(quoteIdentifier(cols.get(i)));
        }
        return sb.toString();
    }

    private static String placeholders(int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            if (i > 0) sb.append(", ");
            sb.append("?");
        }
        return sb.toString();
    }

    private static String mapDataType(String dataType, String udtName,
                                       Object charMaxLen, Object numPrec, Object numScale) {
        if (dataType == null) return "text";
        switch (dataType.toLowerCase()) {
            case "integer": case "int": case "int4": return "integer";
            case "bigint": case "int8": return "bigint";
            case "smallint": case "int2": return "smallint";
            case "real": case "float4": return "real";
            case "double precision": case "float8": return "double precision";
            case "numeric": case "decimal":
                // RisingWave does not support numeric(p,s) with precision/scale constraints
                return "numeric";
            case "boolean": case "bool": return "boolean";
            case "character varying": case "varchar":
                // RisingWave does not support varchar(N) with length specification
                return "varchar";
            case "character": case "char":
                // RisingWave does not support CHAR type
                return "varchar";
            case "text": return "text";
            case "date": return "date";
            case "time without time zone": case "time": return "time";
            case "timestamp without time zone": case "timestamp": return "timestamp";
            case "timestamp with time zone": case "timestamptz": return "timestamp with time zone";
            case "bytea": return "bytea";
            case "jsonb": return "jsonb";
            case "json": return "jsonb";
            case "uuid": return "varchar";
            default:
                // Fallback: use the udt_name if available
                if (udtName != null && !udtName.isEmpty()) {
                    return mapDataType(udtName, null, charMaxLen, numPrec, numScale);
                }
                return "text";
        }
    }

    private static String toRisingWaveType(TapField field) {
        String dt = field.getDataType();
        if (dt == null) return "text";
        String lower = dt.toLowerCase();
        // RisingWave does not support varchar(N) or char(N) with length specification
        if (lower.startsWith("varchar(") || lower.startsWith("character varying(")) {
            return "varchar";
        }
        // RisingWave does not support CHAR type, convert to varchar (no length)
        if (lower.startsWith("char(") || lower.equals("char")
                || lower.startsWith("character(") || lower.equals("character")) {
            return "varchar";
        }
        // RisingWave does not support numeric(p,s) with precision/scale
        if (lower.startsWith("numeric(") || lower.startsWith("decimal(")) {
            return "numeric";
        }
        // RisingWave does not support SERIAL type
        if (lower.contains("serial")) {
            return "bigint";
        }
        return dt;
    }

    /**
     * Convert Tapdata PDK value types to JDBC-compatible types.
     * The PostgreSQL JDBC driver cannot infer SQL types for Tapdata-specific classes
     * like DateTime, so we convert them to standard java.sql types.
     */
    private static Object convertValue(Object value) {
        if (value == null) return null;
        if (value instanceof DateTime) {
            return ((DateTime) value).toTimestamp();
        }
        if (value instanceof java.util.Date && !(value instanceof java.sql.Date)
                && !(value instanceof java.sql.Timestamp)) {
            return new java.sql.Timestamp(((java.util.Date) value).getTime());
        }
        return value;
    }

    private static void setParam(PreparedStatement ps, int idx, Object value) throws SQLException {
        Object converted = convertValue(value);
        ps.setObject(idx, converted);
    }

    private static Map<String, Object> normalizeRecordForStreaming(
            Map<String, Object> record, TapTable table, boolean typedColumns) {
        if (record == null) {
            return null;
        }

        LinkedHashMap<String, Object> normalized = new LinkedHashMap<>();
        Map<String, TapField> fields = table == null ? null : table.getNameFieldMap();
        for (Map.Entry<String, Object> entry : record.entrySet()) {
            TapField field = fields == null ? null : fields.get(entry.getKey());
            normalized.put(entry.getKey(), normalizeValueForStreaming(
                    entry.getValue(), field, typedColumns, false));
        }
        return normalized;
    }

    private static Object normalizeValueForStreaming(
            Object value, TapField field, boolean typedColumns,
            boolean stringifyExactNumbers) {
        if (value == null) {
            return null;
        }

        TapType tapType = field == null ? null : field.getTapType();
        if (tapType instanceof TapDate && value instanceof DateTime) {
            java.sql.Date date = ((DateTime) value).toSqlDate();
            return date == null ? null : date.toString();
        }
        if (tapType instanceof TapTime && value instanceof DateTime) {
            java.sql.Time time = ((DateTime) value).toTime();
            return time == null ? null : time.toString();
        }
        if (tapType instanceof TapDateTime && value instanceof DateTime) {
            DateTime dateTime = (DateTime) value;
            if (dateTime.getTimeZone() != null) {
                ZonedDateTime zonedDateTime = dateTime.toZonedDateTime();
                if (zonedDateTime != null) {
                    return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(zonedDateTime.toOffsetDateTime());
                }
            }
            Timestamp timestamp = dateTime.toTimestamp();
            return timestamp == null ? null : timestamp.toString();
        }
        boolean stringifyNumbers = stringifyExactNumbers
                || !typedColumns
                || isDecimalField(field)
                || isJsonLikeField(field, tapType);
        if (stringifyNumbers && value instanceof java.math.BigDecimal) {
            // Webhook JSON-number decoding can round through floating point. Typed NUMERIC columns
            // parse an exact decimal string, while JSONB keeps the exact value as a JSON string.
            return ((java.math.BigDecimal) value).toPlainString();
        }
        if (stringifyNumbers && value instanceof java.math.BigInteger) {
            return value.toString();
        }
        if (typedColumns && value instanceof byte[] && isBinaryField(field)) {
            // The webhook decoder uses PostgreSQL's standard bytea text format by default.
            // Jackson's normal byte[] representation is Base64 and would be stored incorrectly.
            return toPostgresByteaHex((byte[]) value);
        }
        if (isJsonLikeField(field, tapType) && value instanceof CharSequence) {
            String json = value.toString().trim();
            if (json.isEmpty()) {
                return value.toString();
            }
            try {
                return JSON_MAPPER.readValue(json, Object.class);
            } catch (java.io.IOException e) {
                throw new IllegalArgumentException("Invalid JSON value for field "
                        + (field == null ? "<unknown>" : field.getName()), e);
            }
        }

        if (value instanceof Map<?, ?>) {
            LinkedHashMap<String, Object> normalized = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                String key = entry.getKey() == null ? "null" : entry.getKey().toString();
                normalized.put(key, normalizeValueForStreaming(
                        entry.getValue(), null, typedColumns, stringifyNumbers));
            }
            return normalized;
        }
        if (value instanceof Collection<?>) {
            List<Object> normalized = new ArrayList<>();
            for (Object element : (Collection<?>) value) {
                normalized.add(normalizeValueForStreaming(
                        element, null, typedColumns, stringifyNumbers));
            }
            return normalized;
        }
        if (value.getClass().isArray() && !(value instanceof byte[])) {
            int length = java.lang.reflect.Array.getLength(value);
            List<Object> normalized = new ArrayList<>(length);
            for (int i = 0; i < length; i++) {
                normalized.add(normalizeValueForStreaming(
                        java.lang.reflect.Array.get(value, i), null, typedColumns, stringifyNumbers));
            }
            return normalized;
        }
        if (value instanceof DateTime) {
            Timestamp timestamp = ((DateTime) value).toTimestamp();
            return timestamp == null ? null : timestamp.toString();
        }

        return value;
    }

    private static boolean isJsonLikeField(TapField field, TapType tapType) {
        if (tapType instanceof TapJson || tapType instanceof TapMap) {
            return true;
        }
        if (field == null || field.getDataType() == null) {
            return false;
        }
        String dataType = field.getDataType().toLowerCase(Locale.ROOT);
        return dataType.contains("json");
    }

    private static boolean isDecimalField(TapField field) {
        if (field == null || field.getDataType() == null) {
            return false;
        }
        String dataType = field.getDataType().toLowerCase(Locale.ROOT);
        return dataType.startsWith("numeric") || dataType.startsWith("decimal");
    }

    private static boolean isBinaryField(TapField field) {
        return field != null && field.getDataType() != null
                && "bytea".equalsIgnoreCase(field.getDataType().trim());
    }

    private static String toPostgresByteaHex(byte[] bytes) {
        char[] hex = new char[2 + bytes.length * 2];
        hex[0] = '\\';
        hex[1] = 'x';
        final char[] digits = "0123456789abcdef".toCharArray();
        for (int i = 0; i < bytes.length; i++) {
            int value = bytes[i] & 0xff;
            hex[2 + i * 2] = digits[value >>> 4];
            hex[3 + i * 2] = digits[value & 0x0f];
        }
        return new String(hex);
    }

    private static void closeQuietly(AutoCloseable c) {
        if (c != null) {
            try { c.close(); } catch (Exception ignored) {}
        }
    }
}
