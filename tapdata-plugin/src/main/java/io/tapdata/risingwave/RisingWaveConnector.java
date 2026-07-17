package io.tapdata.risingwave;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
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
    static final int MAX_CACHED_WS_CLIENTS = 100;
    private Connection connection;
    private String schema;

    /** "jdbc", "streaming", or "streaming_jsonb" */
    private String ingestMode;

    /** Only used in streaming mode. Per-table WsIngestClient cache. */
    private final Map<String, WsClientEntry> wsClients =
            new LinkedHashMap<>(16, 0.75f, true);
    private String wsIngestEndpoint;
    private String wsDatabase;
    private String wsWebhookSecret;
    private String wsWebhookSecretName;
    private final AtomicBoolean alive = new AtomicBoolean(false);
    /** Serializes access to the shared JDBC connection on the write path. */
    private final Object jdbcWriteLock = new Object();

    // ---- TapNode lifecycle ----

    public static void debugLog(String msg) {
        TapLogger.debug(TAG, "{}", msg);
    }

    @Override
    public void init(TapConnectionContext context) throws Throwable {
        closeResources();
        debugLog("init() called");
        DataMap cfg = context.getConnectionConfig();
        RisingWaveConfig config = RisingWaveConfig.from(cfg);
        this.schema = config.schema();
        this.ingestMode = config.ingestMode();

        if (isWebSocketMode(ingestMode)) {
            this.wsIngestEndpoint = config.resolvedIngestEndpoint();
            this.wsDatabase = config.database();
            this.wsWebhookSecret = config.webhookSecret();
            this.wsWebhookSecretName = config.webhookSecretName();
            debugLog("init() streaming mode, ingestEndpoint=" + wsIngestEndpoint + " db=" + wsDatabase);
        }

        // Always open a JDBC connection for schema discovery and DDL operations. Resolve all
        // WebSocket configuration first so a configuration failure cannot leak this connection.
        this.connection = RisingWaveJdbc.open(config);
        alive.set(true);
        debugLog("init() done, schema=" + schema + " ingestMode=" + ingestMode);
    }

    @Override
    public void stop(TapConnectionContext context) throws Throwable {
        closeResources();
    }

    private synchronized void closeResources() {
        alive.set(false);
        List<WsClientEntry> clients = new ArrayList<>(wsClients.values());
        wsClients.clear();
        for (WsClientEntry client : clients) {
            try { client.client.close(); } catch (Exception ignored) {}
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
        return RisingWaveConnectionTester.test(context, consumer);
    }

    @Override
    public void discoverSchema(TapConnectionContext context, List<String> tables,
                               int tableSize,
                               Consumer<List<TapTable>> consumer) throws Throwable {
        String schemaName = getSchema(context);
        String tableFilter = RisingWaveSql.buildTableFilter(tables);
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
                "  AND tc.table_name = kcu.table_name " +
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
                    field.setDataType(RisingWaveSql.mapDiscoveredType(
                            rs.getString("data_type"), rs.getString("udt_name")));
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
        if (tapTables.isEmpty() && (tables == null || tables.isEmpty())) {
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
                value == null ? null : value.getValue());
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
                + " pk=" + RisingWaveCdcNormalizer.primaryKeys(table));
        List<WsIngestClient.DmlOperation> operations = new ArrayList<>(events.size());

        for (TapRecordEvent event : events) {
            if (!alive.get()) {
                throw new java.util.concurrent.CancellationException("Connector is stopping");
            }
            try {
                if (event instanceof TapInsertRecordEvent) {
                    TapInsertRecordEvent insert = (TapInsertRecordEvent) event;
                    RisingWaveCdcNormalizer.validateColumns(table, insert.getAfter());
                    Map<String, Object> after = RisingWaveValueConverter.normalizeStreamingRecord(
                            insert.getAfter(), table, true);
                    operations.add(new WsIngestClient.DmlOperation("insert", null, after));
                    inserted++;
                } else if (event instanceof TapUpdateRecordEvent) {
                    TapUpdateRecordEvent updateEvent = (TapUpdateRecordEvent) event;
                    RisingWaveCdcNormalizer.Update update =
                            RisingWaveCdcNormalizer.normalizeUpdate(table, updateEvent);
                    Map<String, Object> oldIdentity = RisingWaveValueConverter.normalizeStreamingRecord(
                            update.oldFilter, table, true);
                    Map<String, Object> rowAfter = RisingWaveValueConverter.normalizeStreamingRecord(
                            update.rowAfter, table, true);
                    if (update.keyChanged) {
                        operations.add(new WsIngestClient.DmlOperation("delete", oldIdentity, null));
                    }
                    operations.add(new WsIngestClient.DmlOperation("update", oldIdentity, rowAfter));
                    updated++;
                } else if (event instanceof TapDeleteRecordEvent) {
                    TapDeleteRecordEvent delete = (TapDeleteRecordEvent) event;
                    Map<String, Object> before = RisingWaveValueConverter.normalizeStreamingRecord(
                            RisingWaveCdcNormalizer.deleteFilter(table, delete.getBefore()),
                            table, true);
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

        // Hold a lease through the ACK so cache eviction cannot close an in-flight stream.
        try (WsClientLease lease = acquireWsClient(table.getId())) {
            List<CompletableFuture<Void>> futures = lease.client().sendBatch(operations);
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(120, java.util.concurrent.TimeUnit.SECONDS);
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

        List<WsIngestClient.DmlOperation> operations = new ArrayList<>(events.size());
        for (TapRecordEvent event : events) {
            if (!alive.get()) {
                throw new java.util.concurrent.CancellationException("Connector is stopping");
            }
            TapInsertRecordEvent insert = (TapInsertRecordEvent) event;
            Map<String, Object> document = RisingWaveValueConverter.normalizeStreamingRecord(
                    insert.getAfter(), table, false);
            operations.add(new WsIngestClient.DmlOperation("insert", null, document));
        }

        try (WsClientLease lease = acquireWsClient(table.getId())) {
            List<CompletableFuture<Void>> futures = lease.client().sendBatch(operations);
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

    /** Acquire a client until the caller has received the ACK for its batch. */
    private synchronized WsClientLease acquireWsClient(String tableId) throws Exception {
        WsClientEntry existing = wsClients.get(tableId);
        if (existing != null) {
            existing.inUse++;
            return new WsClientLease(tableId, existing, true);
        }

        evictIdleWsClientIfFull();

        TableReference table = tableReference(tableId);
        WsIngestClient client = new WsIngestClient(
                wsIngestEndpoint, wsDatabase, table.schema, table.table, wsWebhookSecret);
        client.connect();
        WsClientEntry created = new WsClientEntry(client);
        boolean cached = wsClients.size() < MAX_CACHED_WS_CLIENTS;
        if (cached) {
            created.inUse = 1;
            wsClients.put(tableId, created);
        }
        debugLog("Created WsIngestClient for table=" + tableId + " cached=" + cached
                + " ingestEndpoint=" + wsIngestEndpoint);
        return new WsClientLease(tableId, created, cached);
    }

    private void evictIdleWsClientIfFull() {
        if (wsClients.size() < MAX_CACHED_WS_CLIENTS) {
            return;
        }
        Iterator<Map.Entry<String, WsClientEntry>> iterator = wsClients.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, WsClientEntry> candidate = iterator.next();
            if (candidate.getValue().inUse == 0) {
                iterator.remove();
                candidate.getValue().client.close();
                return;
            }
        }
    }

    /** Remove and close the WsIngestClient for a table (called on error to force reconnect). */
    private synchronized void removeWsClient(String tableId) {
        WsClientEntry client = wsClients.remove(tableId);
        if (client != null) {
            try { client.client.close(); } catch (Exception ignored) {}
        }
    }

    private synchronized void releaseWsClient(String tableId, WsClientEntry entry, boolean cached) {
        if (!cached) {
            entry.client.close();
            return;
        }
        WsClientEntry current = wsClients.get(tableId);
        if (current == entry && current.inUse > 0) {
            current.inUse--;
        }
    }

    // ---- JDBC write path (original) ----

    private void writeRecordJdbc(List<TapRecordEvent> events,
                                   TapTable table,
                                   Consumer<WriteListResult<TapRecordEvent>> resultConsumer) throws Throwable {
        synchronized (jdbcWriteLock) {
            writeRecordJdbcLocked(events, table, resultConsumer);
        }
    }

    private void writeRecordJdbcLocked(List<TapRecordEvent> events,
                                       TapTable table,
                                       Consumer<WriteListResult<TapRecordEvent>> resultConsumer) throws Throwable {
        WriteListResult<TapRecordEvent> result = new WriteListResult<>();
        long inserted = 0, updated = 0, deleted = 0;
        boolean dirty = false;
        boolean keyless = RisingWaveCdcNormalizer.primaryKeys(table).isEmpty();
        Set<List<Object>> pendingInsertIdentities = new HashSet<>();

        debugLog("writeRecord called: " + events.size() + " events for table " + table.getId());
        for (TapRecordEvent event : events) {
            try {
                if (event instanceof TapInsertRecordEvent) {
                    TapInsertRecordEvent insert = (TapInsertRecordEvent) event;
                    RisingWaveCdcNormalizer.validateColumns(table, insert.getAfter());
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
                    TapUpdateRecordEvent updateEvent = (TapUpdateRecordEvent) event;
                    RisingWaveCdcNormalizer.Update update =
                            RisingWaveCdcNormalizer.normalizeUpdate(table, updateEvent);
                    if (dirty && (keyless || !pendingInsertIdentities.isEmpty())) {
                        flushJdbcWrites();
                        dirty = false;
                        pendingInsertIdentities.clear();
                    }
                    if (update.keyChanged) {
                        doInsertOrUpdate(table, update.rowAfter);
                        doDelete(table, update.oldFilter);
                        trackPendingIdentity(table, pendingInsertIdentities, update.rowAfter);
                    } else {
                        int rows = doUpdate(table, update.oldFilter, update.rowAfter);
                        if (rows == 0) {
                            // The normalized row is complete, so a missing keyed row can be recreated.
                            doInsertOrUpdate(table, update.rowAfter);
                            trackPendingIdentity(
                                    table, pendingInsertIdentities, update.rowAfter);
                        }
                    }
                    dirty = true;
                    updated++;
                } else if (event instanceof TapDeleteRecordEvent) {
                    TapDeleteRecordEvent delete = (TapDeleteRecordEvent) event;
                    Map<String, Object> deleteFilter =
                            RisingWaveCdcNormalizer.deleteFilter(table, delete.getBefore());
                    if (dirty && (keyless || !pendingInsertIdentities.isEmpty())) {
                        flushJdbcWrites();
                        dirty = false;
                        pendingInsertIdentities.clear();
                    }
                    doDelete(table, deleteFilter);
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
        Collection<String> primaryKeys = RisingWaveCdcNormalizer.primaryKeys(table);
        if (record == null || primaryKeys == null || primaryKeys.isEmpty()
                || !record.keySet().containsAll(primaryKeys)) {
            return null;
        }
        List<Object> identity = new ArrayList<>(primaryKeys.size());
        for (String primaryKey : primaryKeys) {
            Object value = record.get(primaryKey);
            String type = RisingWaveSql.canonicalType(field(table, primaryKey).getDataType());
            boolean numeric = "smallint".equals(type) || "integer".equals(type)
                    || "bigint".equals(type) || "numeric".equals(type)
                    || "real".equals(type) || "double precision".equals(type);
            if (numeric && value instanceof Number) {
                try {
                    value = new java.math.BigDecimal(value.toString()).stripTrailingZeros();
                } catch (NumberFormatException ignored) {
                    // Keep uncommon non-decimal Number representations unchanged.
                }
            } else if ("bytea".equals(type) && value instanceof byte[]) {
                value = Base64.getEncoder().encodeToString((byte[]) value);
            }
            identity.add(value);
        }
        return identity;
    }

    private static void trackPendingIdentity(
            TapTable table,
            Set<List<Object>> pendingInsertIdentities,
            Map<String, Object> record) {
        List<Object> identity = keyedIdentityOf(table, record);
        if (identity != null) {
            pendingInsertIdentities.add(identity);
        }
    }

    private void doInsert(TapTable table, Map<String, Object> record) throws SQLException {
        if (record == null || record.isEmpty()) return;
        List<String> cols = new ArrayList<>(record.keySet());
        String tableName = fullTableName(table.getId());
        String sql = "INSERT INTO " + tableName + " (" +
                RisingWaveSql.quoteColumns(cols) + ") VALUES (" +
                RisingWaveSql.placeholders(cols.size()) + ")";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (int i = 0; i < cols.size(); i++) {
                String column = cols.get(i);
                RisingWaveValueConverter.setJdbcParameter(
                        ps, i + 1, record.get(column), field(table, column));
            }
            ps.executeUpdate();
        }
    }

    private int doUpdate(TapTable table, Map<String, Object> before, Map<String, Object> after) throws SQLException {
        Collection<String> pks = RisingWaveCdcNormalizer.primaryKeys(table);
        if (before == null || before.isEmpty() || after == null || after.isEmpty()) {
            return 0;
        }
        List<String> setCols = new ArrayList<>();
        for (String column : after.keySet()) {
            if (pks == null || !pks.contains(column)) {
                setCols.add(column);
            }
        }
        if (setCols.isEmpty()) {
            return pks != null && !pks.isEmpty() && rowExistsByPrimaryKey(table, before, pks)
                    ? 1 : 0;
        }
        List<String> filterCols = pks != null && !pks.isEmpty()
                ? new ArrayList<>(pks) : new ArrayList<>(before.keySet());
        if (filterCols.isEmpty()) return 0;

        String tableName = fullTableName(table.getId());
        StringBuilder sql = new StringBuilder("UPDATE ").append(tableName).append(" SET ");
        for (int i = 0; i < setCols.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append(RisingWaveSql.quoteIdentifier(setCols.get(i))).append(" = ?");
        }
        sql.append(" WHERE ");
        for (int i = 0; i < filterCols.size(); i++) {
            if (i > 0) sql.append(" AND ");
            String column = filterCols.get(i);
            sql.append(RisingWaveSql.quoteIdentifier(column));
            if (before.get(column) == null) {
                sql.append(" IS NULL");
            } else {
                sql.append(" = ?");
            }
        }

        try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            int idx = 1;
            for (String col : setCols) {
                RisingWaveValueConverter.setJdbcParameter(
                        ps, idx++, after.get(col), field(table, col));
            }
            for (String column : filterCols) {
                Object value = before.get(column);
                if (value != null) {
                    RisingWaveValueConverter.setJdbcParameter(
                            ps, idx++, value, field(table, column));
                }
            }
            return ps.executeUpdate();
        }
    }

    private void doInsertOrUpdate(TapTable table, Map<String, Object> record) throws SQLException {
        Collection<String> pks = RisingWaveCdcNormalizer.primaryKeys(table);
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
            sql.append(RisingWaveSql.quoteIdentifier(column));
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
                    RisingWaveValueConverter.setJdbcParameter(
                            statement, index++, value, field(table, column));
                }
            }
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next();
            }
        }
    }

    private void doDelete(TapTable table, Map<String, Object> record) throws SQLException {
        Collection<String> pks = RisingWaveCdcNormalizer.primaryKeys(table);
        if (record == null) return;

        String tableName = fullTableName(table.getId());
        // Use PKs if available, otherwise use all columns as filter
        List<String> filterCols = (pks != null && !pks.isEmpty())
                ? new ArrayList<>(pks) : new ArrayList<>(record.keySet());

        StringBuilder sql = new StringBuilder("DELETE FROM ").append(tableName).append(" WHERE ");
        for (int i = 0; i < filterCols.size(); i++) {
            if (i > 0) sql.append(" AND ");
            sql.append(RisingWaveSql.quoteIdentifier(filterCols.get(i)));
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
                if (val != null) {
                    RisingWaveValueConverter.setJdbcParameter(
                            ps, idx++, val, field(table, col));
                }
            }
            ps.executeUpdate();
        }
    }

    private void flushJdbcWrites() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("FLUSH");
        }
    }

    private static TapField field(TapTable table, String column) {
        return table == null || table.getNameFieldMap() == null
                ? null : table.getNameFieldMap().get(column);
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
        if (tableExists(connection, tableReference)) {
            if (MODE_STREAMING_JSONB.equals(ingestMode)) {
                validateExistingJsonbTable(connection, tableReference);
            } else {
                validateExistingTable(connection, tableReference, table);
            }
            if (isWebSocketMode(ingestMode)
                    && wsWebhookSecret != null && !wsWebhookSecret.isEmpty()) {
                RisingWaveWebhookSecret.prepare(connection, tableReference.schema, tableReference.table,
                        wsWebhookSecretName, wsWebhookSecret);
            }
            opts.tableExists(true);
            return opts;
        }
        RisingWaveWebhookSecret.Handle secretHandle = isWebSocketMode(ingestMode)
                ? RisingWaveWebhookSecret.prepare(connection, tableReference.schema, tableReference.table,
                        wsWebhookSecretName, wsWebhookSecret)
                : RisingWaveWebhookSecret.Handle.disabled();
        try {
            return createNewTable(table, tableReference, secretHandle, opts);
        } catch (Throwable error) {
            try {
                RisingWaveWebhookSecret.dropManaged(
                        connection, tableReference.schema, secretHandle);
            } catch (SQLException cleanupError) {
                error.addSuppressed(cleanupError);
            }
            throw error;
        }
    }

    private io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions createNewTable(
            TapTable table,
            TableReference tableReference,
            RisingWaveWebhookSecret.Handle secretHandle,
            io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions opts) throws SQLException {
        LinkedHashMap<String, TapField> fields = table.getNameFieldMap();
        StringBuilder sql = new StringBuilder("CREATE TABLE ")
                .append(tableReference.qualifiedName()).append(" (");

        if (MODE_STREAMING_JSONB.equals(ingestMode)) {
            sql.append(RisingWaveSql.quoteIdentifier(JSONB_PAYLOAD_COLUMN)).append(" JSONB)")
                    .append(" WITH (connector = 'webhook')")
                    .append(RisingWaveSql.webhookValidationClause(
                            secretHandle.name(), JSONB_PAYLOAD_COLUMN));
            executeCreateTable(sql.toString(), table.getId(), opts);
            return opts;
        }

        Collection<String> pks = RisingWaveCdcNormalizer.primaryKeys(table);
        List<String> colDefs = new ArrayList<>();
        for (Map.Entry<String, TapField> entry : fields.entrySet()) {
            TapField f = entry.getValue();
            String colDef = RisingWaveSql.quoteIdentifier(f.getName()) + " "
                    + RisingWaveSql.targetType(f);
            if (!MODE_STREAMING.equals(ingestMode) && Boolean.FALSE.equals(f.getNullable())) {
                colDef += " NOT NULL";
            }
            colDefs.add(colDef);
        }
        sql.append(String.join(", ", colDefs));

        if (pks != null && !pks.isEmpty()) {
            sql.append(", PRIMARY KEY (")
                    .append(RisingWaveSql.quoteColumns(new ArrayList<>(pks))).append(")");
        }
        sql.append(")");
        if (MODE_STREAMING.equals(ingestMode)) {
            sql.append(" WITH (connector = 'webhook')");
            sql.append(RisingWaveSql.webhookValidationClause(secretHandle.name(), null));
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
        synchronized (jdbcWriteLock) {
            try (Statement st = connection.createStatement()) {
                st.execute("DELETE FROM " + tableName);
                // clearTable may be followed immediately by WebSocket writes, which use a
                // different protocol and connection. FLUSH is the cross-protocol visibility
                // barrier that prevents old rows from briefly coexisting with the reload.
                st.execute("FLUSH");
            }
        }
    }

    private void dropTable(TapConnectorContext context,
                           io.tapdata.entity.event.ddl.table.TapDropTableEvent event) throws Throwable {
        removeWsClient(event.getTableId());
        TableReference table = tableReference(event.getTableId());
        synchronized (jdbcWriteLock) {
            try (Statement st = connection.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table.qualifiedName());
            }
            if (isWebSocketMode(ingestMode)
                    && wsWebhookSecret != null && !wsWebhookSecret.isEmpty()
                    && (wsWebhookSecretName == null || wsWebhookSecretName.isEmpty())) {
                RisingWaveWebhookSecret.dropAutomatic(
                        connection, table.schema, table.table);
            }
        }
    }

    // ---- Helpers ----

    private String getSchema(TapConnectionContext context) {
        if (schema != null && !schema.isEmpty()) return schema;
        return configuredSchema(context.getConnectionConfig());
    }

    private static String configuredSchema(DataMap cfg) {
        return RisingWaveConfig.from(cfg).schema();
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
            String expectedType = RisingWaveSql.canonicalType(
                    RisingWaveSql.targetType(expected.getValue()));
            String actualType = RisingWaveSql.canonicalType(existingColumns.get(requiredColumn));
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
        expectedPrimaryKeys.addAll(RisingWaveCdcNormalizer.primaryKeys(expectedTable));
        Set<String> existingPrimaryKeys = loadPrimaryKeys(conn, table);
        if (!expectedPrimaryKeys.equals(existingPrimaryKeys)) {
            throw new SQLException("Existing target table " + table.qualifiedName()
                    + " has primary key " + existingPrimaryKeys
                    + " but the Tapdata model requires " + expectedPrimaryKeys);
        }

        if (isWebSocketMode(ingestMode)) {
            validateWebhookBackedTable(conn, table);
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
                || !"jsonb".equals(RisingWaveSql.canonicalType(
                        columns.get(JSONB_PAYLOAD_COLUMN)))) {
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
            boolean configuredSecret = wsWebhookSecret != null && !wsWebhookSecret.isEmpty();
            boolean validatesRequests = normalizedDdl.contains("validate ");
            if (!configuredSecret && validatesRequests) {
                throw new SQLException("Existing target table " + table.qualifiedName()
                        + " requires webhook validation, but Webhook Secret is empty");
            }
            if (configuredSecret) {
                String expectedSecret = wsWebhookSecretName == null || wsWebhookSecretName.isEmpty()
                        ? RisingWaveWebhookSecret.automaticName(table.schema, table.table)
                        : wsWebhookSecretName;
                if (!RisingWaveSql.referencesWebhookSecret(ddl, expectedSecret)) {
                    throw new SQLException("Existing target table " + table.qualifiedName()
                            + " does not reference the configured protected RisingWave Secret \""
                            + expectedSecret + "\"; recreate the table or align Webhook Secret Name");
                }
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
        Collection<String> primaryKeys = RisingWaveCdcNormalizer.primaryKeys(table);
        if (primaryKeys.isEmpty()) {
            String tableName = table == null ? "<unknown>" : table.getId();
            throw new IllegalArgumentException("WebSocket streaming requires a primary key for table "
                    + tableName + " so updates, deletes, and retries preserve row identity");
        }
    }

    private static final class WsClientEntry {
        private final WsIngestClient client;
        private int inUse;

        private WsClientEntry(WsIngestClient client) {
            this.client = client;
        }
    }

    private final class WsClientLease implements AutoCloseable {
        private final String tableId;
        private final WsClientEntry entry;
        private final boolean cached;
        private boolean released;

        private WsClientLease(String tableId, WsClientEntry entry, boolean cached) {
            this.tableId = tableId;
            this.entry = entry;
            this.cached = cached;
        }

        private WsIngestClient client() {
            return entry.client;
        }

        @Override
        public void close() {
            if (!released) {
                released = true;
                releaseWsClient(tableId, entry, cached);
            }
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
            return RisingWaveSql.quoteIdentifier(schema) + "."
                    + RisingWaveSql.quoteIdentifier(table);
        }
    }

    private static void closeQuietly(AutoCloseable c) {
        if (c != null) {
            try { c.close(); } catch (Exception ignored) {}
        }
    }
}
