package io.tapdata.risingwave;

import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.risingwave.streaming.WsIngestClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Runs non-destructive metadata checks and temporary-table write probes for connection setup. */
final class RisingWaveConnectionTester {
    private static final Pattern VERSION_PATTERN = Pattern.compile(
            "(?i)RisingWave[-/\\s]+v?(\\d+)\\.(\\d+)(?:\\.(\\d+))?");

    private RisingWaveConnectionTester() {
    }

    static ConnectionOptions test(TapConnectionContext context, Consumer<TestItem> consumer) {
        ConnectionOptions options = ConnectionOptions.create();
        RisingWaveConfig config;
        try {
            config = RisingWaveConfig.from(context.getConnectionConfig());
            options.connectionString(config.connectionString())
                    .instanceUniqueId(config.instanceUniqueId())
                    .namespaces(Collections.singletonList(config.schema()));
        } catch (Exception e) {
            RisingWaveConnector.debugLog("connectionTest() configuration ERROR: "
                    + e.getClass().getName() + ": " + e.getMessage());
            consumer.accept(new TestItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED,
                    "Invalid connection configuration: " + e.getMessage()));
            return options;
        }

        RisingWaveConnector.debugLog("connectionTest() start host=" + config.host()
                + " port=" + config.port()
                + " database=" + config.database()
                + " schema=" + config.schema()
                + " ingestMode=" + config.ingestMode());
        try (Connection connection = RisingWaveJdbc.open(config)) {
            consumer.accept(new TestItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY,
                    "Connected to RisingWave"));
            String version = testVersion(connection, config.ingestMode(), consumer);
            if (version != null) {
                options.setDbVersion(parseRisingWaveVersionString(version));
            }
            if (testSchema(connection, config.schema(), consumer)) {
                if (RisingWaveConnector.isWebSocketMode(config.ingestMode())) {
                    testStreamingWrite(connection, config, consumer);
                } else {
                    testJdbcWritePrivilege(connection, config.schema(), consumer);
                }
            } else {
                consumer.accept(new TestItem(TestItem.ITEM_WRITE, TestItem.RESULT_FAILED,
                        "Write access cannot be verified because schema \"" + config.schema()
                                + "\" is unavailable"));
                if (RisingWaveConnector.isWebSocketMode(config.ingestMode())) {
                    consumer.accept(new TestItem(RisingWaveConnector.INGEST_ENDPOINT_TEST_ITEM,
                            TestItem.RESULT_FAILED,
                            "WebSocket ingest cannot be verified because schema \""
                                    + config.schema() + "\" is unavailable"));
                }
            }
        } catch (Exception e) {
            RisingWaveConnector.debugLog("connectionTest() JDBC ERROR: "
                    + e.getClass().getName() + ": " + e.getMessage());
            consumer.accept(new TestItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED,
                    "Connection failed: " + e.getMessage()));
        }
        return options;
    }

    private static String testVersion(
            Connection connection, String mode, Consumer<TestItem> consumer) {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT version()")) {
            String version = resultSet.next() ? resultSet.getString(1) : "unknown";
            RisingWaveConnector.debugLog("connectionTest() version=" + version);
            if (RisingWaveConnector.isWebSocketMode(mode) && !supportsWebSocketIngest(version)) {
                consumer.accept(new TestItem(TestItem.ITEM_VERSION, TestItem.RESULT_FAILED,
                        "WebSocket streaming requires RisingWave "
                                + RisingWaveConnector.MINIMUM_WEBSOCKET_VERSION
                                + " or later; server reported: " + version));
            } else {
                consumer.accept(new TestItem(TestItem.ITEM_VERSION,
                        TestItem.RESULT_SUCCESSFULLY, version));
            }
            return version;
        } catch (Exception e) {
            RisingWaveConnector.debugLog("connectionTest() version ERROR: "
                    + e.getClass().getName() + ": " + e.getMessage());
            consumer.accept(new TestItem(TestItem.ITEM_VERSION, TestItem.RESULT_FAILED,
                    "Version check failed: " + e.getMessage()));
            return null;
        }
    }

    private static boolean testSchema(
            Connection connection, String schema, Consumer<TestItem> consumer) {
        String sql = "SELECT count(*) FROM rw_catalog.rw_schemas WHERE name = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, schema);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next() && resultSet.getInt(1) > 0) {
                    consumer.accept(new TestItem(RisingWaveConnector.SCHEMA_TEST_ITEM,
                            TestItem.RESULT_SUCCESSFULLY, "Schema \"" + schema + "\" exists"));
                    return true;
                }
            }
            consumer.accept(new TestItem(RisingWaveConnector.SCHEMA_TEST_ITEM,
                    TestItem.RESULT_FAILED, "Schema \"" + schema + "\" does not exist"));
        } catch (Exception e) {
            RisingWaveConnector.debugLog("connectionTest() schema ERROR: "
                    + e.getClass().getName() + ": " + e.getMessage());
            consumer.accept(new TestItem(RisingWaveConnector.SCHEMA_TEST_ITEM,
                    TestItem.RESULT_FAILED, "Schema check failed: " + e.getMessage()));
        }
        return false;
    }

    private static void testJdbcWritePrivilege(
            Connection connection, String schema, Consumer<TestItem> consumer) {
        String table = "tap___test_" + UUID.randomUUID().toString().replace("-", "");
        String qualifiedTable = RisingWaveSql.quoteIdentifier(schema) + "."
                + RisingWaveSql.quoteIdentifier(table);
        boolean created = false;
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE " + qualifiedTable
                    + " (id BIGINT PRIMARY KEY, probe_value VARCHAR)");
            created = true;
            statement.executeUpdate("INSERT INTO " + qualifiedTable + " VALUES (1, 'initial')");
            statement.execute("FLUSH");
            statement.executeUpdate("UPDATE " + qualifiedTable
                    + " SET probe_value = 'updated' WHERE id = 1");
            statement.execute("FLUSH");
            try (ResultSet resultSet = statement.executeQuery(
                    "SELECT probe_value FROM " + qualifiedTable + " WHERE id = 1")) {
                if (!resultSet.next() || !"updated".equals(resultSet.getString(1))) {
                    throw new java.sql.SQLException(
                            "Updated probe row was not query-visible after FLUSH");
                }
            }
            statement.executeUpdate("DELETE FROM " + qualifiedTable + " WHERE id = 1");
            statement.execute("FLUSH");
            try (ResultSet resultSet = statement.executeQuery(
                    "SELECT count(*) FROM " + qualifiedTable)) {
                if (!resultSet.next() || resultSet.getLong(1) != 0) {
                    throw new java.sql.SQLException(
                            "Deleted probe row remained query-visible after FLUSH");
                }
            }
            statement.execute("DROP TABLE " + qualifiedTable);
            created = false;
            consumer.accept(new TestItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY,
                    "Create, insert, update, delete, and drop succeeded in schema \""
                            + schema + "\""));
        } catch (Exception e) {
            RisingWaveConnector.debugLog("connectionTest() write privilege ERROR: "
                    + e.getClass().getName() + ": " + e.getMessage());
            consumer.accept(new TestItem(TestItem.ITEM_WRITE, TestItem.RESULT_FAILED,
                    "Write privilege check failed in schema \"" + schema + "\": "
                            + e.getMessage()));
        } finally {
            if (created) {
                dropProbeTable(connection, qualifiedTable);
            }
        }
    }

    private static void testStreamingWrite(
            Connection connection, RisingWaveConfig config, Consumer<TestItem> consumer) {
        boolean jsonbMode = RisingWaveConnector.MODE_STREAMING_JSONB.equals(config.ingestMode());
        String table = "tap___test_" + UUID.randomUUID().toString().replace("-", "");
        String qualifiedTable = RisingWaveSql.quoteIdentifier(config.schema()) + "."
                + RisingWaveSql.quoteIdentifier(table);
        boolean created = false;
        Exception ddlError = null;
        Exception ingestError = null;
        try (Statement statement = connection.createStatement()) {
            String columns = jsonbMode
                    ? "(" + RisingWaveSql.quoteIdentifier(RisingWaveConnector.JSONB_PAYLOAD_COLUMN)
                            + " JSONB)"
                    : "(id BIGINT, probe_value VARCHAR, PRIMARY KEY (id))";
            statement.execute("CREATE TABLE " + qualifiedTable + " " + columns
                    + " WITH (connector = 'webhook')"
                    + RisingWaveSql.webhookValidationClause(config.webhookSecret(),
                            jsonbMode ? RisingWaveConnector.JSONB_PAYLOAD_COLUMN : null));
            created = true;

            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", 1L);
            row.put("probe_value", "websocket_precheck");
            try (WsIngestClient client = new WsIngestClient(config.resolvedIngestEndpoint(),
                    config.database(), config.schema(), table, config.webhookSecret())) {
                client.connect();
                List<CompletableFuture<Void>> futures = client.sendBatch(Collections.singletonList(
                        new WsIngestClient.DmlOperation("insert", null, row)));
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                        .get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                ingestError = e;
                RisingWaveConnector.debugLog("connectionTest() websocket ingest ERROR endpoint="
                        + config.resolvedIngestEndpoint() + ": " + e.getClass().getName()
                        + ": " + e.getMessage());
            }
        } catch (Exception e) {
            ddlError = e;
            RisingWaveConnector.debugLog("connectionTest() streaming DDL ERROR table="
                    + qualifiedTable + ": " + e.getClass().getName() + ": " + e.getMessage());
        } finally {
            if (created) {
                try (Statement cleanup = connection.createStatement()) {
                    cleanup.execute("DROP TABLE " + qualifiedTable);
                } catch (Exception cleanupError) {
                    ddlError = cleanupError;
                    RisingWaveConnector.debugLog("connectionTest() streaming probe cleanup ERROR table="
                            + qualifiedTable + ": " + cleanupError.getMessage());
                }
            }
        }

        if (ddlError == null) {
            consumer.accept(new TestItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY,
                    "Create and drop of a webhook-backed table succeeded in schema \""
                            + config.schema() + "\""));
        } else {
            consumer.accept(new TestItem(TestItem.ITEM_WRITE, TestItem.RESULT_FAILED,
                    "Webhook table create/drop check failed in schema \"" + config.schema()
                            + "\": " + ddlError.getMessage()));
        }

        if (!created) {
            consumer.accept(new TestItem(RisingWaveConnector.INGEST_ENDPOINT_TEST_ITEM,
                    TestItem.RESULT_FAILED,
                    "WebSocket ingest cannot be verified because the temporary webhook table "
                            + "could not be created"));
        } else if (ingestError == null) {
            consumer.accept(new TestItem(RisingWaveConnector.INGEST_ENDPOINT_TEST_ITEM,
                    TestItem.RESULT_SUCCESSFULLY,
                    "WebSocket connection, signed init, DML write, and RisingWave ACK succeeded"));
        } else {
            consumer.accept(new TestItem(RisingWaveConnector.INGEST_ENDPOINT_TEST_ITEM,
                    TestItem.RESULT_FAILED,
                    "WebSocket ingest check failed: " + ingestError.getMessage()));
        }
    }

    static boolean supportsWebSocketIngest(String versionOutput) {
        int[] version = parseRisingWaveVersion(versionOutput);
        return version != null
                && (version[0] > 3 || (version[0] == 3 && version[1] >= 0));
    }

    static int[] parseRisingWaveVersion(String versionOutput) {
        String version = parseRisingWaveVersionString(versionOutput);
        if (version == null) {
            return null;
        }
        String[] components = version.split("\\.");
        return new int[]{Integer.parseInt(components[0]), Integer.parseInt(components[1]),
                Integer.parseInt(components[2])};
    }

    static String parseRisingWaveVersionString(String versionOutput) {
        if (versionOutput == null) {
            return null;
        }
        Matcher matcher = VERSION_PATTERN.matcher(versionOutput);
        if (!matcher.find()) {
            return null;
        }
        int patch = matcher.group(3) == null ? 0 : Integer.parseInt(matcher.group(3));
        return matcher.group(1) + "." + matcher.group(2) + "." + patch;
    }

    private static void dropProbeTable(Connection connection, String qualifiedTable) {
        try (Statement cleanup = connection.createStatement()) {
            cleanup.execute("DROP TABLE IF EXISTS " + qualifiedTable);
        } catch (Exception cleanupError) {
            RisingWaveConnector.debugLog("connectionTest() write probe cleanup ERROR table="
                    + qualifiedTable + ": " + cleanupError.getMessage());
        }
    }
}
