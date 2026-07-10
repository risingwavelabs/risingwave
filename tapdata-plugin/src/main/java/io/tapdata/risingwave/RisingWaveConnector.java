package io.tapdata.risingwave;

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
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.risingwave.streaming.WsIngestClient;

import java.net.URI;
import java.sql.*;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Tapdata PDK connector for RisingWave streaming database.
 *
 * Supports two write modes:
 * <ul>
 *   <li><b>jdbc</b> (default) — Standard PostgreSQL JDBC inserts/updates/deletes.
 *       Compatible with all RisingWave deployments.</li>
 *   <li><b>streaming</b> — High-throughput WebSocket streaming DML over the
 *       RisingWave ingest endpoint.  Sends DML messages asynchronously and
 *       waits for per-epoch acks before advancing the offset.  Requires the
 *       RisingWave webhook/ingest service to be reachable on {@code ingestEndpoint}.</li>
 * </ul>
 */
@TapConnectorClass("spec_risingwave.json")
public class RisingWaveConnector implements TapConnector {

    private Connection connection;
    private String schema;

    /** "jdbc" or "streaming" */
    private String ingestMode;

    /** Only used in streaming mode. Per-table WsIngestClient cache. */
    private final Map<String, WsIngestClient> wsClients = new LinkedHashMap<>();
    private String wsIngestEndpoint;
    private String wsDatabase;
    private String wsWebhookSecret;

    // ---- TapNode lifecycle ----

    public static void debugLog(String msg) {
        try {
            java.io.FileWriter fw = new java.io.FileWriter("/tmp/rw_connector.log", true);
            fw.write(new java.util.Date() + " " + msg + "\n");
            fw.close();
        } catch (Exception ignore) {}
    }

    @Override
    public void init(TapConnectionContext context) throws Throwable {
        debugLog("init() called");
        DataMap cfg = context.getConnectionConfig();
        this.schema = cfg.getString("schema");
        if (schema == null || schema.isEmpty()) {
            schema = "public";
        }
        this.ingestMode = cfg.getString("ingest_mode");
        if (ingestMode == null || ingestMode.isEmpty()) {
            ingestMode = "jdbc";
        }

        // Always open a JDBC connection for schema discovery and DDL operations.
        this.connection = openConnection(context);

        if ("streaming".equals(ingestMode)) {
            this.wsIngestEndpoint = cfg.getString("ingestEndpoint");
            this.wsDatabase = cfg.getString("database");
            this.wsWebhookSecret = cfg.getString("webhookSecret");
            if (wsIngestEndpoint == null || wsIngestEndpoint.isEmpty()) {
                wsIngestEndpoint = "ws://" + cfg.getString("host") + ":4560";
            }
            if (wsDatabase == null || wsDatabase.isEmpty()) wsDatabase = "dev";
            debugLog("init() streaming mode, ingestEndpoint=" + wsIngestEndpoint + " db=" + wsDatabase);
        }

        debugLog("init() done, schema=" + schema + " ingestMode=" + ingestMode);
    }

    @Override
    public void stop(TapConnectionContext context) throws Throwable {
        for (WsIngestClient client : wsClients.values()) {
            try { client.close(); } catch (Exception ignored) {}
        }
        wsClients.clear();
        closeQuietly(connection);
        connection = null;
    }

    // ---- TapConnectorNode ----

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext context,
                                            Consumer<TestItem> consumer) throws Throwable {
        ConnectionOptions options = ConnectionOptions.create();
        DataMap cfg = context.getConnectionConfig();
        debugLog("connectionTest() start host=" + cfg.getString("host")
                + " port=" + cfg.getObject("port")
                + " database=" + cfg.getString("database")
                + " schema=" + cfg.getString("schema")
                + " ingestMode=" + cfg.getString("ingest_mode"));
        // Always test JDBC connectivity for schema discovery and DDL operations.
        try (Connection conn = openConnection(context)) {
            consumer.accept(new TestItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY,
                    "Connected to RisingWave"));
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT version()")) {
                String version = rs.next() ? rs.getString(1) : "unknown";
                debugLog("connectionTest() version=" + version);
                consumer.accept(new TestItem(TestItem.ITEM_VERSION, TestItem.RESULT_SUCCESSFULLY, version));
            }
            consumer.accept(new TestItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY,
                    "Write access verified"));
        } catch (Exception e) {
            debugLog("connectionTest() JDBC ERROR: " + e.getClass().getName() + ": " + e.getMessage());
            consumer.accept(new TestItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED,
                    "Connection failed: " + e.getMessage()));
        }

        // In streaming mode, also verify that the WebSocket ingest endpoint is reachable.
        String mode = cfg.getString("ingest_mode");
        if ("streaming".equals(mode)) {
            String ingestEndpoint = cfg.getString("ingestEndpoint");
            if (ingestEndpoint == null || ingestEndpoint.isEmpty()) {
                ingestEndpoint = "ws://" + cfg.getString("host") + ":4560";
            }
            String database = cfg.getString("database");
            if (database == null || database.isEmpty()) database = "dev";
            String schemaName = getSchema(context);
            try {
                URI uri = URI.create(ingestEndpoint);
                int port = uri.getPort() >= 0 ? uri.getPort() : ("wss".equalsIgnoreCase(uri.getScheme()) ? 443 : 80);
                java.net.Socket socket = new java.net.Socket();
                socket.connect(new java.net.InetSocketAddress(uri.getHost(), port), 5000);
                socket.close();
                debugLog("connectionTest() ingest endpoint reachable endpoint=" + ingestEndpoint
                        + " database=" + database + " schema=" + schemaName);
                consumer.accept(new TestItem("ingest_endpoint", TestItem.RESULT_SUCCESSFULLY,
                        "WebSocket ingest endpoint reachable"));
            } catch (Exception e) {
                debugLog("connectionTest() ingest endpoint ERROR endpoint=" + ingestEndpoint
                        + ": " + e.getClass().getName() + ": " + e.getMessage());
                consumer.accept(new TestItem("ingest_endpoint", TestItem.RESULT_FAILED,
                        "WebSocket ingest endpoint check failed: " + e.getMessage()));
            }
        }

        return options;
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
    }

    // ---- Write Record ----

    private void writeRecord(TapConnectorContext context,
                             List<TapRecordEvent> events,
                             TapTable table,
                             Consumer<WriteListResult<TapRecordEvent>> resultConsumer) throws Throwable {
        debugLog("writeRecord() mode=" + ingestMode + " tableId="
                + (table == null ? "null" : table.getId()) + " events=" + events.size());
        if ("streaming".equals(ingestMode)) {
            writeRecordStreaming(events, table, resultConsumer);
        } else {
            writeRecordJdbc(events, table, resultConsumer);
        }
    }

    // ---- Streaming (WebSocket) write path ----

    private void writeRecordStreaming(List<TapRecordEvent> events,
                                       TapTable table,
                                       Consumer<WriteListResult<TapRecordEvent>> resultConsumer) throws Throwable {
        WriteListResult<TapRecordEvent> result = new WriteListResult<>();
        long inserted = 0, updated = 0, deleted = 0;

        debugLog("writeRecordStreaming start table=" + table.getId()
                + " events=" + events.size()
                + " pk=" + table.primaryKeys());
        WsIngestClient client = getOrCreateWsClient(table.getId());
        List<WsIngestClient.DmlOperation> operations = new ArrayList<>(events.size());

        for (TapRecordEvent event : events) {
            try {
                if (event instanceof TapInsertRecordEvent) {
                    TapInsertRecordEvent insert = (TapInsertRecordEvent) event;
                    Map<String, Object> after = normalizeRecordForStreaming(insert.getAfter(), table);
                    debugLog("writeRecordStreaming insert after=" + after);
                    operations.add(new WsIngestClient.DmlOperation("insert", null, after));
                    inserted++;
                } else if (event instanceof TapUpdateRecordEvent) {
                    TapUpdateRecordEvent update = (TapUpdateRecordEvent) event;
                    Map<String, Object> before = normalizeRecordForStreaming(update.getBefore(), table);
                    Map<String, Object> after = normalizeRecordForStreaming(update.getAfter(), table);
                    debugLog("writeRecordStreaming update before=" + before
                            + " after=" + after);
                    operations.add(new WsIngestClient.DmlOperation("update", before, after));
                    updated++;
                } else if (event instanceof TapDeleteRecordEvent) {
                    TapDeleteRecordEvent delete = (TapDeleteRecordEvent) event;
                    Map<String, Object> before = normalizeRecordForStreaming(delete.getBefore(), table);
                    debugLog("writeRecordStreaming delete before=" + before);
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

    private String findAnyTableName(Connection conn, String schemaName) {
        String sql = "SELECT table_name FROM information_schema.tables "
                + "WHERE table_schema = ? AND table_type = 'BASE TABLE' "
                + "ORDER BY table_name LIMIT 1";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        } catch (Exception e) {
            debugLog("findAnyTableName() ERROR schema=" + schemaName + ": " + e.getMessage());
            return null;
        }
    }

    // ---- JDBC write path (original) ----

    private void writeRecordJdbc(List<TapRecordEvent> events,
                                   TapTable table,
                                   Consumer<WriteListResult<TapRecordEvent>> resultConsumer) throws Throwable {
        WriteListResult<TapRecordEvent> result = new WriteListResult<>();
        long inserted = 0, updated = 0, deleted = 0;

        debugLog("writeRecord called: " + events.size() + " events for table " + table.getId());

        for (TapRecordEvent event : events) {
            try {
                if (event instanceof TapInsertRecordEvent) {
                    TapInsertRecordEvent insert = (TapInsertRecordEvent) event;
                    doInsert(table, insert.getAfter());
                    inserted++;
                } else if (event instanceof TapUpdateRecordEvent) {
                    TapUpdateRecordEvent update = (TapUpdateRecordEvent) event;
                    int rows = doUpdate(table, update.getBefore(), update.getAfter());
                    if (rows == 0) {
                        // Fall back to upsert
                        doInsertOrUpdate(table, update.getAfter());
                    }
                    updated++;
                } else if (event instanceof TapDeleteRecordEvent) {
                    TapDeleteRecordEvent delete = (TapDeleteRecordEvent) event;
                    doDelete(table, delete.getBefore());
                    deleted++;
                }
            } catch (Exception e) {
                debugLog("writeRecord ERROR: " + e.getClass().getName() + ": " + e.getMessage());
                result.addError(event, e);
            }
        }

        debugLog("writeRecord done: inserted=" + inserted + " updated=" + updated + " deleted=" + deleted);
        resultConsumer.accept(result.insertedCount(inserted)
                .modifiedCount(updated)
                .removedCount(deleted));
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
        Collection<String> pks = table.primaryKeys();
        if (pks == null || pks.isEmpty() || before == null) {
            return 0;
        }
        Map<String, Object> record = after != null ? after : before;
        List<String> setCols = new ArrayList<>();
        for (String col : record.keySet()) {
            if (!pks.contains(col)) {
                setCols.add(col);
            }
        }
        if (setCols.isEmpty()) return 0;

        String tableName = fullTableName(table.getId());
        StringBuilder sql = new StringBuilder("UPDATE ").append(tableName).append(" SET ");
        for (int i = 0; i < setCols.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append('"').append(setCols.get(i)).append('"').append(" = ?");
        }
        sql.append(" WHERE ");
        List<String> pkList = new ArrayList<>(pks);
        for (int i = 0; i < pkList.size(); i++) {
            if (i > 0) sql.append(" AND ");
            sql.append('"').append(pkList.get(i)).append('"').append(" = ?");
        }

        try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            int idx = 1;
            for (String col : setCols) {
                setParam(ps, idx++, record.get(col));
            }
            // Use before values for PK lookup, fallback to after
            Map<String, Object> pkSource = before != null ? before : after;
            for (String pk : pkList) {
                setParam(ps, idx++, pkSource != null ? pkSource.get(pk) : null);
            }
            return ps.executeUpdate();
        }
    }

    private void doInsertOrUpdate(TapTable table, Map<String, Object> record) throws SQLException {
        Collection<String> pks = table.primaryKeys();
        if (pks == null || pks.isEmpty() || record == null) {
            doInsert(table, record);
            return;
        }
        List<String> cols = new ArrayList<>(record.keySet());
        String tableName = fullTableName(table.getId());

        // Try INSERT ON CONFLICT DO UPDATE (works if table has PK)
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableName)
                .append(" (").append(quoteCols(cols)).append(") VALUES (")
                .append(placeholders(cols.size())).append(")");

        List<String> nonPkCols = new ArrayList<>();
        for (String col : cols) {
            if (!pks.contains(col)) nonPkCols.add(col);
        }

        if (!nonPkCols.isEmpty()) {
            sql.append(" ON CONFLICT (").append(quoteCols(new ArrayList<>(pks))).append(") DO UPDATE SET ");
            for (int i = 0; i < nonPkCols.size(); i++) {
                if (i > 0) sql.append(", ");
                sql.append('"').append(nonPkCols.get(i)).append('"')
                        .append(" = EXCLUDED.\"").append(nonPkCols.get(i)).append('"');
            }
        } else {
            sql.append(" ON CONFLICT DO NOTHING");
        }

        try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            for (int i = 0; i < cols.size(); i++) {
                setParam(ps, i + 1, record.get(cols.get(i)));
            }
            ps.executeUpdate();
        }
    }

    private void doDelete(TapTable table, Map<String, Object> record) throws SQLException {
        Collection<String> pks = table.primaryKeys();
        if (record == null) return;

        String tableName = fullTableName(table.getId());
        // Use PKs if available, otherwise use all columns as filter
        List<String> filterCols = (pks != null && !pks.isEmpty())
                ? new ArrayList<>(pks) : new ArrayList<>(record.keySet());

        StringBuilder sql = new StringBuilder("DELETE FROM ").append(tableName).append(" WHERE ");
        for (int i = 0; i < filterCols.size(); i++) {
            if (i > 0) sql.append(" AND ");
            sql.append('"').append(filterCols.get(i)).append('"');
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

    // ---- DDL ----

    private io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions createTable(
            TapConnectorContext context,
            io.tapdata.entity.event.ddl.table.TapCreateTableEvent event) throws Throwable {
        io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions opts =
                io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions.create();
        TapTable table = event.getTable();
        if (table == null) return opts;

        String tableName = fullTableName(table.getId());
        StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (");

        LinkedHashMap<String, TapField> fields = table.getNameFieldMap();
        if (fields == null || fields.isEmpty()) {
            debugLog("createTable() empty fields table=" + table.getId());
            return opts;
        }

        Collection<String> pks = table.primaryKeys();
        List<String> colDefs = new ArrayList<>();
        for (Map.Entry<String, TapField> entry : fields.entrySet()) {
            TapField f = entry.getValue();
            String colDef = '"' + f.getName() + "\" " + toRisingWaveType(f);
            if (!"streaming".equals(ingestMode) && Boolean.FALSE.equals(f.getNullable())) {
                colDef += " NOT NULL";
            }
            colDefs.add(colDef);
        }
        sql.append(String.join(", ", colDefs));

        if (pks != null && !pks.isEmpty()) {
            sql.append(", PRIMARY KEY (").append(quoteCols(new ArrayList<>(pks))).append(")");
        }
        sql.append(")");
        if ("streaming".equals(ingestMode)) {
            sql.append(" WITH (connector = 'webhook')");
            // If a webhook secret is configured, add VALIDATE clause so the table
            // requires HMAC-SHA256 signature verification on the WebSocket init frame.
            // This matches the WsIngestClient signing logic.
            if (wsWebhookSecret != null && !wsWebhookSecret.isEmpty()) {
                String escapedSecret = wsWebhookSecret.replace("'", "''");
                sql.append(" VALIDATE AS secure_compare(")
                   .append("headers->>'x-rw-signature', ")
                   .append("'sha256=' || encode(hmac('").append(escapedSecret)
                   .append("', payload, 'sha256'), 'hex'))");
            }
        }

        debugLog("createTable() sql=" + sql);
        try (Statement st = connection.createStatement()) {
            st.execute(sql.toString());
            debugLog("createTable() success table=" + table.getId());
        } catch (Exception e) {
            debugLog("createTable() ERROR table=" + table.getId() + ": "
                    + e.getClass().getName() + ": " + e.getMessage());
            throw e;
        }
        opts.tableExists(false);
        return opts;
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
        DataMap cfg = context.getConnectionConfig();
        String s = cfg.getString("schema");
        return (s != null && !s.isEmpty()) ? s : "public";
    }

    private String fullTableName(String tableId) {
        // tableId may already include schema, or may not
        if (tableId != null && tableId.contains(".")) {
            String[] parts = tableId.split("\\.", 2);
            return '"' + parts[0] + "\".\"" + parts[1] + '"';
        }
        return '"' + schema + "\".\"" + tableId + '"';
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
            sb.append('"').append(cols.get(i)).append('"');
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

    private static Map<String, Object> normalizeRecordForStreaming(Map<String, Object> record, TapTable table) {
        if (record == null) {
            return null;
        }

        LinkedHashMap<String, Object> normalized = new LinkedHashMap<>();
        Map<String, TapField> fields = table == null ? null : table.getNameFieldMap();
        for (Map.Entry<String, Object> entry : record.entrySet()) {
            TapField field = fields == null ? null : fields.get(entry.getKey());
            normalized.put(entry.getKey(), normalizeValueForStreaming(entry.getValue(), field));
        }
        return normalized;
    }

    private static Object normalizeValueForStreaming(Object value, TapField field) {
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
        if (isJsonLikeField(field, tapType) && value instanceof CharSequence) {
            String json = value.toString().trim();
            return json.isEmpty() ? value.toString() : WsIngestClient.rawJson(json);
        }

        if (value instanceof Map<?, ?>) {
            LinkedHashMap<String, Object> normalized = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                String key = entry.getKey() == null ? "null" : entry.getKey().toString();
                normalized.put(key, normalizeValueForStreaming(entry.getValue(), null));
            }
            return normalized;
        }
        if (value instanceof Collection<?>) {
            List<Object> normalized = new ArrayList<>();
            for (Object element : (Collection<?>) value) {
                normalized.add(normalizeValueForStreaming(element, null));
            }
            return normalized;
        }
        if (value.getClass().isArray() && !(value instanceof byte[])) {
            int length = java.lang.reflect.Array.getLength(value);
            List<Object> normalized = new ArrayList<>(length);
            for (int i = 0; i < length; i++) {
                normalized.add(normalizeValueForStreaming(java.lang.reflect.Array.get(value, i), null));
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

    private static void closeQuietly(AutoCloseable c) {
        if (c != null) {
            try { c.close(); } catch (Exception ignored) {}
        }
    }
}
