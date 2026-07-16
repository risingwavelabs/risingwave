package io.tapdata.risingwave;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.risingwave.streaming.WsIngestClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class RisingWaveConnectionTestIT {

    @BeforeAll
    static void requireLiveRisingWave() {
        assumeTrue(Boolean.getBoolean("risingwave.it"),
                "Set -Drisingwave.it=true to run against a local RisingWave instance");
    }

    @Test
    void validatesConnectionVersionSchemaWriteAndWebSocketEndpoint() throws Throwable {
        List<TestItem> items = runConnectionTest("public", "streaming");

        assertSuccessful(items, TestItem.ITEM_CONNECTION);
        assertSuccessful(items, TestItem.ITEM_VERSION);
        assertSuccessful(items, RisingWaveConnector.SCHEMA_TEST_ITEM);
        assertSuccessful(items, TestItem.ITEM_WRITE);
        assertSuccessful(items, RisingWaveConnector.INGEST_ENDPOINT_TEST_ITEM);
        assertEquals(0, writeProbeTableCount());
    }

    @Test
    void returnsStablePdkConnectionIdentityAndServerMetadata() throws Throwable {
        TapConnectionContext context = connectionContext(
                "public", "jdbc", "root", "", "ws://127.0.0.1:4560", "");
        List<TestItem> items = new ArrayList<>();

        ConnectionOptions options = new RisingWaveConnector().connectionTest(context, items::add);

        assertSuccessful(items, TestItem.ITEM_CONNECTION);
        assertEquals("127.0.0.1:4566/dev/public", options.getConnectionString());
        assertEquals(64, options.getInstanceUniqueId().length());
        assertEquals(java.util.Collections.singletonList("public"), options.getNamespaces());
        assertEquals("3.1.0", options.getDbVersion());
    }

    @Test
    void defaultsToStreamingModeWhenWriteModeIsUnset() throws Throwable {
        List<TestItem> items = runConnectionTest("public", null);

        assertSuccessful(items, TestItem.ITEM_CONNECTION);
        assertSuccessful(items, TestItem.ITEM_VERSION);
        assertSuccessful(items, TestItem.ITEM_WRITE);
        assertSuccessful(items, RisingWaveConnector.INGEST_ENDPOINT_TEST_ITEM);
        assertEquals(0, writeProbeTableCount());
    }

    @Test
    void acceptsIanaTimezoneWithoutUrlDecodingIt() throws Throwable {
        TapConnectionContext context = connectionContext(
                "public", "jdbc", "root", "", "ws://127.0.0.1:4560", "");
        context.getConnectionConfig().put("timezone", "Asia/Shanghai");
        RisingWaveConnector connector = new RisingWaveConnector();
        List<TestItem> items = new ArrayList<>();

        connector.connectionTest(context, items::add);

        assertSuccessful(items, TestItem.ITEM_CONNECTION);
        assertSuccessful(items, TestItem.ITEM_WRITE);
        assertEquals(0, writeProbeTableCount());
    }

    @Test
    void validatesSignedWebSocketPrecheck() throws Throwable {
        List<TestItem> items = runConnectionTest(
                "public", "streaming", "root", "",
                "ws://127.0.0.1:4560", "tapdata_precheck_secret");

        assertSuccessful(items, TestItem.ITEM_WRITE);
        assertSuccessful(items, RisingWaveConnector.INGEST_ENDPOINT_TEST_ITEM);
        assertEquals(0, writeProbeTableCount());
    }

    @Test
    void validatesSignedJsonbAppendOnlyWebSocketPrecheck() throws Throwable {
        List<TestItem> items = runConnectionTest(
                "public", "streaming_jsonb", "root", "",
                "ws://127.0.0.1:4560", "tapdata_jsonb_precheck_secret");

        assertSuccessful(items, TestItem.ITEM_VERSION);
        assertSuccessful(items, TestItem.ITEM_WRITE);
        assertSuccessful(items, RisingWaveConnector.INGEST_ENDPOINT_TEST_ITEM);
        assertEquals(0, writeProbeTableCount());
    }

    @Test
    void signedWebhookTableReferencesCatalogSecretWithoutExposingValue() throws Throwable {
        String tableName = "tapdata_secret_" + shortSuffix();
        String secretValue = "tapdata-secret-'" + shortSuffix();
        String secretName = RisingWaveWebhookSecret.automaticName("public", tableName);
        RisingWaveConnector connector = new RisingWaveConnector();
        TapConnectionContext context = connectionContext(
                "public", "streaming", "root", "",
                "ws://127.0.0.1:4560", secretValue);
        try {
            connector.init(context);
            TapTable table = new TapTable(tableName)
                    .add(new TapField("id", "integer").isPrimaryKey(true).primaryKeyPos(1));
            connector.createTable(null, new TapCreateTableEvent().table(table));

            try (Connection connection = rootConnection(); Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(
                         "SHOW CREATE TABLE public.\"" + tableName + "\"")) {
                assertTrue(resultSet.next());
                String ddl = resultSet.getString(2);
                assertTrue(ddl.contains("VALIDATE SECRET"));
                assertTrue(ddl.contains(secretName));
                assertTrue(!ddl.contains(secretValue));
            }

            ConnectorFunctions functions = new ConnectorFunctions();
            connector.registerCapabilities(functions, new TapCodecsRegistry());
            TapDropTableEvent drop = new TapDropTableEvent();
            drop.setTableId(tableName);
            functions.getDropTableFunction().dropTable(null, drop);
            assertEquals(0, querySecretCount(secretName));
        } finally {
            connector.stop(context);
            dropTable(tableName);
        }
    }

    @Test
    void signedPrecheckCreatesAndRotatesConfiguredNamedSecret() throws Throwable {
        String secretName = "tapdata_existing_secret_" + shortSuffix();
        String secretValue = "existing-secret-" + shortSuffix();

        try {
            TapConnectionContext context = connectionContext(
                    "public", "streaming", "root", "",
                    "ws://127.0.0.1:4560", secretValue);
            context.getConnectionConfig().put("webhookSecretName", secretName);
            List<TestItem> items = new ArrayList<>();

            new RisingWaveConnector().connectionTest(context, items::add);

            assertSuccessful(items, TestItem.ITEM_WRITE);
            assertSuccessful(items, RisingWaveConnector.INGEST_ENDPOINT_TEST_ITEM);
            assertEquals(1, querySecretCount(secretName));
        } finally {
            try (Connection connection = rootConnection(); Statement statement = connection.createStatement()) {
                statement.execute("DROP SECRET IF EXISTS public.\"" + secretName + "\"");
            }
        }
    }

    @Test
    void restartingAgainstExistingTableRotatesManagedWebhookSecret() throws Throwable {
        String tableName = "tapdata_secret_rotate_" + shortSuffix();
        TapTable table = new TapTable(tableName)
                .add(new TapField("id", "integer").isPrimaryKey(true).primaryKeyPos(1))
                .add(new TapField("name", "varchar"));
        TapConnectionContext firstContext = connectionContext(
                "public", "streaming", "root", "",
                "ws://127.0.0.1:4560", "first-secret");
        RisingWaveConnector firstConnector = new RisingWaveConnector();
        try {
            firstConnector.init(firstContext);
            firstConnector.createTable(null, new TapCreateTableEvent().table(table));
        } finally {
            firstConnector.stop(firstContext);
        }

        TapConnectionContext secondContext = connectionContext(
                "public", "streaming", "root", "",
                "ws://127.0.0.1:4560", "second-secret");
        RisingWaveConnector secondConnector = new RisingWaveConnector();
        try {
            secondConnector.init(secondContext);
            assertTrue(secondConnector.createTable(
                    null, new TapCreateTableEvent().table(table)).getTableExists());
            ConnectorFunctions functions = new ConnectorFunctions();
            secondConnector.registerCapabilities(functions, new TapCodecsRegistry());
            functions.getWriteRecordFunction().writeRecord(null, Collections.singletonList(
                    TapInsertRecordEvent.create().table(tableName).after(record(1, "rotated"))),
                    table, ignored -> { });
            awaitName(tableName, 1, "rotated");
        } finally {
            secondConnector.stop(secondContext);
            dropTable(tableName);
        }
    }

    @Test
    void reportsUnreachableWebSocketEndpointAndCleansUpProbeTable() throws Throwable {
        List<TestItem> items = runConnectionTest(
                "public", "streaming", "root", "",
                "ws://127.0.0.1:1", "");

        assertSuccessful(items, TestItem.ITEM_WRITE);
        assertFailed(items, RisingWaveConnector.INGEST_ENDPOINT_TEST_ITEM);
        assertEquals(0, writeProbeTableCount());
    }

    @Test
    void reportsMissingSchemaAndSkipsWriteProbe() throws Throwable {
        List<TestItem> items = runConnectionTest("tapdata_schema_does_not_exist", "jdbc");

        assertSuccessful(items, TestItem.ITEM_CONNECTION);
        assertSuccessful(items, TestItem.ITEM_VERSION);
        assertFailed(items, RisingWaveConnector.SCHEMA_TEST_ITEM);
        assertFailed(items, TestItem.ITEM_WRITE);
        assertEquals(0, writeProbeTableCount());
    }

    @Test
    void requestedMissingTableDoesNotFallBackToEveryTableInSchema() throws Throwable {
        RisingWaveConnector connector = new RisingWaveConnector();
        TapConnectionContext context = connectionContext(
                "public", "jdbc", "root", "", "ws://127.0.0.1:4560", "");
        List<TapTable> discovered = new ArrayList<>();
        try {
            connector.init(context);
            connector.discoverSchema(context,
                    Collections.singletonList("tapdata_table_does_not_exist"), 100,
                    discovered::addAll);
            assertTrue(discovered.isEmpty());
        } finally {
            connector.stop(context);
        }
    }

    @Test
    void reportsMissingWritePrivilegeForVisibleSchema() throws Throwable {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String user = "tapdata_no_write_" + suffix;
        try (Connection connection = rootConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE USER \"" + user + "\" WITH PASSWORD 'tapdata_test'");
        }

        try {
            List<TestItem> items = runConnectionTest(
                    "public", "jdbc", user, "tapdata_test");

            assertSuccessful(items, TestItem.ITEM_CONNECTION);
            assertSuccessful(items, RisingWaveConnector.SCHEMA_TEST_ITEM);
            assertFailed(items, TestItem.ITEM_WRITE);
        } finally {
            try (Connection connection = rootConnection();
                 Statement statement = connection.createStatement()) {
                statement.execute("DROP USER IF EXISTS \"" + user + "\"");
            }
        }
    }

    @Test
    void reportsMissingStreamingDdlPrivilegeAndSkipsWebSocketProbe() throws Throwable {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String user = "tapdata_no_streaming_write_" + suffix;
        try (Connection connection = rootConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE USER \"" + user + "\" WITH PASSWORD 'tapdata_test'");
        }

        try {
            List<TestItem> items = runConnectionTest(
                    "public", "streaming", user, "tapdata_test",
                    "ws://127.0.0.1:4560", "");

            assertSuccessful(items, TestItem.ITEM_CONNECTION);
            assertSuccessful(items, RisingWaveConnector.SCHEMA_TEST_ITEM);
            assertFailed(items, TestItem.ITEM_WRITE);
            assertFailed(items, RisingWaveConnector.INGEST_ENDPOINT_TEST_ITEM);
            assertEquals(0, writeProbeTableCount());
        } finally {
            try (Connection connection = rootConnection();
                 Statement statement = connection.createStatement()) {
                statement.execute("DROP USER IF EXISTS \"" + user + "\"");
            }
        }
    }

    @Test
    void acceptsAnExistingCompatibleWebhookTable() throws Throwable {
        String tableName = "tapdata_existing_webhook_" + shortSuffix();
        try (Connection connection = rootConnection(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE public.\"" + tableName
                    + "\" (id integer, PRIMARY KEY (id)) WITH (connector = 'webhook')");
        }

        RisingWaveConnector connector = new RisingWaveConnector();
        TapConnectionContext context = connectionContext("public", "streaming", "root", "",
                "ws://127.0.0.1:4560", "");
        try {
            connector.init(context);
            TapTable table = new TapTable(tableName).add(new TapField("id", "integer").isPrimaryKey(true));
            assertTrue(connector.createTable(null, new TapCreateTableEvent().table(table)).getTableExists());
        } finally {
            connector.stop(context);
            dropTable(tableName);
        }
    }

    @Test
    void rejectsAnExistingNonWebhookTableInStreamingMode() throws Throwable {
        String tableName = "tapdata_existing_plain_" + shortSuffix();
        try (Connection connection = rootConnection(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE public.\"" + tableName + "\" (id integer PRIMARY KEY)");
        }

        RisingWaveConnector connector = new RisingWaveConnector();
        TapConnectionContext context = connectionContext("public", "streaming", "root", "",
                "ws://127.0.0.1:4560", "");
        try {
            connector.init(context);
            TapTable table = new TapTable(tableName).add(new TapField("id", "integer").isPrimaryKey(true));
            SQLException error = assertThrows(SQLException.class,
                    () -> connector.createTable(null, new TapCreateTableEvent().table(table)));
            assertTrue(error.getMessage().contains("not webhook-backed"));
        } finally {
            connector.stop(context);
            dropTable(tableName);
        }
    }

    @Test
    void rejectsAnExistingWebhookTableWithMismatchedPrimaryKey() throws Throwable {
        String tableName = "tapdata_existing_pk_mismatch_" + shortSuffix();
        try (Connection connection = rootConnection(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE public.\"" + tableName
                    + "\" (id integer, name varchar, PRIMARY KEY (name)) WITH (connector = 'webhook')");
        }

        RisingWaveConnector connector = new RisingWaveConnector();
        TapConnectionContext context = connectionContext("public", "streaming", "root", "",
                "ws://127.0.0.1:4560", "");
        try {
            connector.init(context);
            TapTable table = new TapTable(tableName)
                    .add(new TapField("id", "integer").isPrimaryKey(true).primaryKeyPos(1))
                    .add(new TapField("name", "varchar"));
            SQLException error = assertThrows(SQLException.class,
                    () -> connector.createTable(null, new TapCreateTableEvent().table(table)));
            assertTrue(error.getMessage().contains("has primary key [name]"));
            assertTrue(error.getMessage().contains("requires [id]"));
        } finally {
            connector.stop(context);
            dropTable(tableName);
        }
    }

    @Test
    void rejectsAnExistingWebhookTableWithMismatchedColumnType() throws Throwable {
        String tableName = "tapdata_existing_type_mismatch_" + shortSuffix();
        try (Connection connection = rootConnection(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE public.\"" + tableName
                    + "\" (id boolean, PRIMARY KEY (id)) WITH (connector = 'webhook')");
        }

        RisingWaveConnector connector = new RisingWaveConnector();
        TapConnectionContext context = connectionContext("public", "streaming", "root", "",
                "ws://127.0.0.1:4560", "");
        try {
            connector.init(context);
            TapTable table = new TapTable(tableName)
                    .add(new TapField("id", "integer").isPrimaryKey(true).primaryKeyPos(1));
            SQLException error = assertThrows(SQLException.class,
                    () -> connector.createTable(null, new TapCreateTableEvent().table(table)));
            assertTrue(error.getMessage().contains("id (expected integer, found boolean)"));
        } finally {
            connector.stop(context);
            dropTable(tableName);
        }
    }

    @Test
    void rejectsKeylessModelsBeforeCreatingAWebhookTable() throws Throwable {
        String tableName = "tapdata_keyless_rejected_" + shortSuffix();
        RisingWaveConnector connector = new RisingWaveConnector();
        TapConnectionContext context = connectionContext("public", "streaming", "root", "",
                "ws://127.0.0.1:4560", "");
        try {
            connector.init(context);
            TapTable table = new TapTable(tableName).add(new TapField("id", "integer"));
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                    () -> connector.createTable(null, new TapCreateTableEvent().table(table)));
            assertTrue(error.getMessage().contains("requires a primary key"));
            assertEquals(0, queryTableCount(tableName));
        } finally {
            connector.stop(context);
            dropTable(tableName);
        }
    }

    @Test
    void websocketSupportsSameKeyUpdatePrimaryKeyChangeAndDelete() throws Exception {
        String tableName = "tapdata_ws_dml_" + shortSuffix();
        try (Connection connection = rootConnection(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE public.\"" + tableName
                    + "\" (id integer, name varchar, PRIMARY KEY (id)) WITH (connector = 'webhook')");
        }

        try (WsIngestClient client = new WsIngestClient(
                "ws://127.0.0.1:4560", "dev", "public", tableName, "")) {
            client.connect();
            Map<String, Object> first = record(1, "first");
            await(client.sendBatch(Collections.singletonList(
                    new WsIngestClient.DmlOperation("insert", null, first))));
            awaitName(tableName, 1, "first");

            Map<String, Object> sameKeyUpdate = record(1, "updated");
            await(client.sendBatch(Collections.singletonList(
                    new WsIngestClient.DmlOperation("update", first, sameKeyUpdate))));
            awaitName(tableName, 1, "updated");
            assertEquals(1, queryCount(tableName));

            Map<String, Object> changedKey = record(2, "moved");
            await(client.sendBatch(java.util.Arrays.asList(
                    new WsIngestClient.DmlOperation("delete", sameKeyUpdate, null),
                    new WsIngestClient.DmlOperation("update", sameKeyUpdate, changedKey))));
            awaitName(tableName, 2, "moved");
            assertEquals(1, queryCount(tableName));

            await(client.sendBatch(Collections.singletonList(
                    new WsIngestClient.DmlOperation("delete", changedKey, null))));
            awaitCount(tableName, 0);
        } finally {
            dropTable(tableName);
        }
    }

    @Test
    void websocketStreamingSplitsBatchesLargerThanTheServerFrameLimit() throws Exception {
        String tableName = "tapdata_ws_split_" + shortSuffix();
        String payload = "x".repeat(3 * 1024 * 1024);
        try (Connection connection = rootConnection(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE public.\"" + tableName
                    + "\" (id integer, payload varchar, PRIMARY KEY (id)) WITH (connector = 'webhook')");
        }

        try (WsIngestClient client = new WsIngestClient(
                "ws://127.0.0.1:4560", "dev", "public", tableName, "")) {
            client.connect();
            List<WsIngestClient.DmlOperation> operations = new ArrayList<>();
            for (int id = 1; id <= 6; id++) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("id", id);
                row.put("payload", payload);
                operations.add(new WsIngestClient.DmlOperation("insert", null, row));
            }
            await(client.sendBatch(operations));
            awaitCount(tableName, 6);
        } finally {
            dropTable(tableName);
        }
    }

    @Test
    void websocketStreamingCompletesPartialUpdatesAndPreservesExplicitNulls() throws Throwable {
        String tableName = "tapdata_ws_partial_" + shortSuffix();
        RisingWaveConnector connector = new RisingWaveConnector();
        TapConnectionContext context = connectionContext(
                "public", "streaming", "root", "", "ws://127.0.0.1:4560", "");
        TapTable table = new TapTable(tableName)
                .add(new TapField("id", "integer").isPrimaryKey(true).primaryKeyPos(1))
                .add(new TapField("name", "varchar"))
                .add(new TapField("quantity", "integer"));
        try {
            connector.init(context);
            connector.createTable(null, new TapCreateTableEvent().table(table));
            ConnectorFunctions functions = new ConnectorFunctions();
            connector.registerCapabilities(functions, new TapCodecsRegistry());

            Map<String, Object> first = new LinkedHashMap<>();
            first.put("id", 1);
            first.put("name", "before");
            first.put("quantity", 42);
            functions.getWriteRecordFunction().writeRecord(null, Collections.singletonList(
                    TapInsertRecordEvent.create().table(tableName).after(first)), table, ignored -> { });

            Map<String, Object> partial = new LinkedHashMap<>();
            partial.put("id", 1);
            partial.put("name", "after");
            functions.getWriteRecordFunction().writeRecord(null, Collections.singletonList(
                    TapUpdateRecordEvent.create().table(tableName).before(first).after(partial)),
                    table, ignored -> { });
            awaitQuantity(tableName, 1, 42);

            Map<String, Object> explicitNull = new LinkedHashMap<>();
            explicitNull.put("id", 1);
            explicitNull.put("quantity", null);
            Map<String, Object> afterPartial = new LinkedHashMap<>();
            afterPartial.put("id", 1);
            afterPartial.put("name", "after");
            afterPartial.put("quantity", 42);
            functions.getWriteRecordFunction().writeRecord(null, Collections.singletonList(
                    TapUpdateRecordEvent.create().table(tableName)
                            .before(afterPartial).after(explicitNull)), table, ignored -> { });
            awaitQuantity(tableName, 1, null);

            Map<String, Object> beforeRemoval = new LinkedHashMap<>();
            beforeRemoval.put("id", 1);
            beforeRemoval.put("name", "after");
            beforeRemoval.put("quantity", 55);
            functions.getWriteRecordFunction().writeRecord(null, Collections.singletonList(
                    TapUpdateRecordEvent.create().table(tableName)
                            .before(explicitNull).after(beforeRemoval)), table, ignored -> { });
            awaitQuantity(tableName, 1, 55);

            Map<String, Object> afterRemoval = new LinkedHashMap<>();
            afterRemoval.put("id", 1);
            afterRemoval.put("name", "after");
            functions.getWriteRecordFunction().writeRecord(null, Collections.singletonList(
                    TapUpdateRecordEvent.create().table(tableName)
                            .before(beforeRemoval).after(afterRemoval)
                            .removedFields(Collections.singletonList("quantity"))),
                    table, ignored -> { });
            awaitQuantity(tableName, 1, null);
        } finally {
            connector.stop(context);
            dropTable(tableName);
        }
    }

    @Test
    void websocketStreamingWritesQualifiedTableIdsToTheirDeclaredSchema() throws Throwable {
        String targetSchema = "tapdata_ws_schema_" + shortSuffix();
        String tableName = "orders";
        String tableId = targetSchema + "." + tableName;
        RisingWaveConnector connector = new RisingWaveConnector();
        TapConnectionContext context = connectionContext(
                "public", "streaming", "root", "", "ws://127.0.0.1:4560", "");
        TapTable table = new TapTable(tableId)
                .add(new TapField("id", "integer").isPrimaryKey(true).primaryKeyPos(1))
                .add(new TapField("name", "varchar"));
        try (Connection connection = rootConnection(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + RisingWaveSql.quoteIdentifier(targetSchema));
            connector.init(context);
            connector.createTable(null, new TapCreateTableEvent().table(table));
            ConnectorFunctions functions = new ConnectorFunctions();
            connector.registerCapabilities(functions, new TapCodecsRegistry());
            functions.getWriteRecordFunction().writeRecord(null, Collections.singletonList(
                    TapInsertRecordEvent.create().table(tableId).after(record(1, "qualified"))),
                    table, ignored -> { });

            awaitQualifiedName(targetSchema, tableName, 1, "qualified");
        } finally {
            connector.stop(context);
            try (Connection connection = rootConnection(); Statement statement = connection.createStatement()) {
                statement.execute("DROP SCHEMA IF EXISTS " + RisingWaveSql.quoteIdentifier(targetSchema)
                        + " CASCADE");
            }
        }
    }

    @Test
    void jdbcSupportsSameKeyUpdatePrimaryKeyChangeAndDelete() throws Throwable {
        String tableName = "tapdata_jdbc_dml_" + shortSuffix();
        RisingWaveConnector connector = new RisingWaveConnector();
        TapConnectionContext context = connectionContext(
                "public", "jdbc", "root", "", "ws://127.0.0.1:4560", "");
        try {
            connector.init(context);
            TapTable table = new TapTable(tableName)
                    .add(new TapField("id", "integer").isPrimaryKey(true).primaryKeyPos(1))
                    .add(new TapField("name", "varchar"));
            connector.createTable(null, new TapCreateTableEvent().table(table));
            ConnectorFunctions functions = new ConnectorFunctions();
            connector.registerCapabilities(functions, new TapCodecsRegistry());

            Map<String, Object> first = record(1, "first");
            functions.getWriteRecordFunction().writeRecord(null, Collections.singletonList(
                    TapInsertRecordEvent.create().table(tableName).after(first)), table, ignored -> { });
            awaitName(tableName, 1, "first");

            Map<String, Object> sameKeyUpdate = record(1, "updated");
            functions.getWriteRecordFunction().writeRecord(null, Collections.singletonList(
                    TapUpdateRecordEvent.create().table(tableName)
                            .before(first).after(sameKeyUpdate)), table, ignored -> { });
            awaitName(tableName, 1, "updated");
            assertEquals(1, queryCount(tableName));

            Map<String, Object> changedKey = record(2, "moved");
            functions.getWriteRecordFunction().writeRecord(null, Collections.singletonList(
                    TapUpdateRecordEvent.create().table(tableName)
                            .before(sameKeyUpdate).after(changedKey)), table, ignored -> { });
            awaitName(tableName, 2, "moved");
            assertEquals(1, queryCount(tableName));

            functions.getWriteRecordFunction().writeRecord(null, Collections.singletonList(
                    TapDeleteRecordEvent.create().table(tableName).before(changedKey)), table, ignored -> { });
            awaitCount(tableName, 0);
        } finally {
            connector.stop(context);
            dropTable(tableName);
        }
    }

    @Test
    void jdbcFlushesBeforeAnUpdateThatDependsOnAnUnflushedInsert() throws Throwable {
        String tableName = "tapdata_jdbc_flush_" + shortSuffix();
        RisingWaveConnector connector = new RisingWaveConnector();
        TapConnectionContext context = connectionContext(
                "public", "jdbc", "root", "", "ws://127.0.0.1:4560", "");
        TapTable table = new TapTable(tableName)
                .add(new TapField("id", "integer").isPrimaryKey(true).primaryKeyPos(1))
                .add(new TapField("name", "varchar"));
        try {
            connector.init(context);
            connector.createTable(null, new TapCreateTableEvent().table(table));
            ConnectorFunctions functions = new ConnectorFunctions();
            connector.registerCapabilities(functions, new TapCodecsRegistry());

            Map<String, Object> inserted = record(1, "inserted");
            Map<String, Object> updated = record(1, "updated");
            functions.getWriteRecordFunction().writeRecord(null, java.util.Arrays.asList(
                    TapInsertRecordEvent.create().table(tableName).after(inserted),
                    TapUpdateRecordEvent.create().table(tableName).before(inserted).after(updated)),
                    table, ignored -> { });
            awaitName(tableName, 1, "updated");
        } finally {
            connector.stop(context);
            dropTable(tableName);
        }
    }

    @Test
    void jdbcUpsertsDuplicateKeysWithinOneBatch() throws Throwable {
        String tableName = "tapdata_jdbc_duplicate_" + shortSuffix();
        RisingWaveConnector connector = new RisingWaveConnector();
        TapConnectionContext context = connectionContext(
                "public", "jdbc", "root", "", "ws://127.0.0.1:4560", "");
        try {
            connector.init(context);
            TapTable table = new TapTable(tableName)
                    .add(new TapField("id", "integer").isPrimaryKey(true).primaryKeyPos(1))
                    .add(new TapField("name", "varchar"));
            connector.createTable(null, new TapCreateTableEvent().table(table));
            ConnectorFunctions functions = new ConnectorFunctions();
            connector.registerCapabilities(functions, new TapCodecsRegistry());

            List<TapRecordEvent> events = java.util.Arrays.asList(
                    TapInsertRecordEvent.create().table(tableName).after(record(1, "first")),
                    TapInsertRecordEvent.create().table(tableName).after(record(1, "second")));
            functions.getWriteRecordFunction().writeRecord(null, events, table, ignored -> { });

            awaitName(tableName, 1, "second");
            assertEquals(1, queryCount(tableName));
        } finally {
            connector.stop(context);
            dropTable(tableName);
        }
    }

    @Test
    void serializesConcurrentJdbcWritesOnTheSharedConnection() throws Throwable {
        String tableName = "tapdata_jdbc_concurrent_" + shortSuffix();
        RisingWaveConnector connector = new RisingWaveConnector();
        TapConnectionContext context = connectionContext(
                "public", "jdbc", "root", "", "ws://127.0.0.1:4560", "");
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            connector.init(context);
            TapTable table = new TapTable(tableName)
                    .add(new TapField("id", "integer").isPrimaryKey(true).primaryKeyPos(1))
                    .add(new TapField("name", "varchar"));
            connector.createTable(null, new TapCreateTableEvent().table(table));
            ConnectorFunctions functions = new ConnectorFunctions();
            connector.registerCapabilities(functions, new TapCodecsRegistry());
            CountDownLatch start = new CountDownLatch(1);

            Future<?> first = executor.submit(() -> writeAfterStart(
                    start, functions, table, record(1, "first")));
            Future<?> second = executor.submit(() -> writeAfterStart(
                    start, functions, table, record(1, "second")));
            start.countDown();
            first.get(30, TimeUnit.SECONDS);
            second.get(30, TimeUnit.SECONDS);

            assertEquals(1, queryCount(tableName));
        } finally {
            executor.shutdownNow();
            connector.stop(context);
            dropTable(tableName);
        }
    }

    @Test
    void clearFlushesBeforeSubsequentWebSocketReload() throws Throwable {
        String tableName = "tapdata_clear_ws_" + shortSuffix();
        RisingWaveConnector connector = new RisingWaveConnector();
        TapConnectionContext context = connectionContext(
                "public", "streaming", "root", "", "ws://127.0.0.1:4560", "");
        try {
            connector.init(context);
            TapTable table = new TapTable(tableName)
                    .add(new TapField("id", "integer").isPrimaryKey(true).primaryKeyPos(1))
                    .add(new TapField("name", "varchar"));
            connector.createTable(null, new TapCreateTableEvent().table(table));
            ConnectorFunctions functions = new ConnectorFunctions();
            connector.registerCapabilities(functions, new TapCodecsRegistry());

            functions.getWriteRecordFunction().writeRecord(null, Collections.singletonList(
                    TapInsertRecordEvent.create().table(tableName).after(record(1, "old"))),
                    table, ignored -> { });
            awaitName(tableName, 1, "old");

            TapClearTableEvent clear = new TapClearTableEvent();
            clear.setTableId(tableName);
            functions.getClearTableFunction().clearTable(null, clear);
            functions.getWriteRecordFunction().writeRecord(null, Collections.singletonList(
                    TapInsertRecordEvent.create().table(tableName).after(record(2, "new"))),
                    table, ignored -> { });

            awaitName(tableName, 2, "new");
            assertEquals(1, queryCount(tableName));
        } finally {
            connector.stop(context);
            dropTable(tableName);
        }
    }

    @Test
    void jdbcUpdatesKeylessRowsUsingTheBeforeImage() throws Throwable {
        String tableName = "tapdata_jdbc_keyless_" + shortSuffix();
        RisingWaveConnector connector = new RisingWaveConnector();
        TapConnectionContext context = connectionContext(
                "public", "jdbc", "root", "", "ws://127.0.0.1:4560", "");
        try {
            connector.init(context);
            TapTable table = new TapTable(tableName)
                    .add(new TapField("id", "integer"))
                    .add(new TapField("name", "varchar"));
            connector.createTable(null, new TapCreateTableEvent().table(table));
            ConnectorFunctions functions = new ConnectorFunctions();
            connector.registerCapabilities(functions, new TapCodecsRegistry());

            Map<String, Object> first = record(1, null);
            functions.getWriteRecordFunction().writeRecord(null, Collections.singletonList(
                    TapInsertRecordEvent.create().table(tableName).after(first)), table, ignored -> { });
            Map<String, Object> updated = record(1, "updated");
            functions.getWriteRecordFunction().writeRecord(null, Collections.singletonList(
                    TapUpdateRecordEvent.create().table(tableName)
                            .before(first).after(updated)), table, ignored -> { });

            awaitName(tableName, 1, "updated");
            assertEquals(1, queryCount(tableName));
        } finally {
            connector.stop(context);
            dropTable(tableName);
        }
    }

    @Test
    void jsonbAppendOnlyModeCreatesKeylessTargetAndWritesWholeDocuments() throws Throwable {
        String tableName = "tapdata_jsonb_append_" + shortSuffix();
        RisingWaveConnector connector = new RisingWaveConnector();
        TapConnectionContext context = connectionContext(
                "public", "streaming_jsonb", "root", "",
                "ws://127.0.0.1:4560", "tapdata_jsonb_append_secret");
        try {
            connector.init(context);
            TapTable sourceTable = new TapTable(tableName)
                    .add(new TapField("id", "integer"))
                    .add(new TapField("name", "varchar"))
                    .add(new TapField("amount", "numeric"));
            connector.createTable(null, new TapCreateTableEvent().table(sourceTable));

            ConnectorFunctions functions = new ConnectorFunctions();
            connector.registerCapabilities(functions, new TapCodecsRegistry());
            List<TapRecordEvent> events = new ArrayList<>();
            Map<String, Object> firstDocument = record(1, "first");
            firstDocument.put("amount", new BigDecimal("1234567890.123456789"));
            events.add(TapInsertRecordEvent.create().table(tableName).after(firstDocument));
            events.add(TapInsertRecordEvent.create().table(tableName).after(record(2, "second")));
            functions.getWriteRecordFunction().writeRecord(null, events, sourceTable, ignored -> { });

            try (Connection connection = rootConnection(); Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(
                         "SELECT data->>'id', data->>'name', data->>'amount', "
                                 + "jsonb_typeof(data->'amount') FROM public.\"" + tableName
                                 + "\" ORDER BY data->>'id'")) {
                assertTrue(resultSet.next());
                assertEquals("1", resultSet.getString(1));
                assertEquals("first", resultSet.getString(2));
                assertEquals("1234567890.123456789", resultSet.getString(3));
                assertEquals("string", resultSet.getString(4));
                assertTrue(resultSet.next());
                assertEquals("2", resultSet.getString(1));
                assertEquals("second", resultSet.getString(2));
                assertTrue(!resultSet.next());
            }
        } finally {
            connector.stop(context);
            dropTable(tableName);
        }
    }

    @Test
    void jsonbAppendOnlyModeStoresBinaryAsPostgresByteaHexText() throws Throwable {
        String tableName = "tapdata_jsonb_binary_" + shortSuffix();
        RisingWaveConnector connector = new RisingWaveConnector();
        TapConnectionContext context = connectionContext(
                "public", "streaming_jsonb", "root", "", "ws://127.0.0.1:4560", "");
        TapTable sourceTable = new TapTable(tableName)
                .add(new TapField("id", "integer"))
                .add(new TapField("payload", "bytea"));
        try {
            connector.init(context);
            connector.createTable(null, new TapCreateTableEvent().table(sourceTable));
            ConnectorFunctions functions = new ConnectorFunctions();
            connector.registerCapabilities(functions, new TapCodecsRegistry());

            Map<String, Object> record = new LinkedHashMap<>();
            record.put("id", 1);
            record.put("payload", new byte[]{0x00, 0x0f, (byte) 0xff});
            functions.getWriteRecordFunction().writeRecord(null, Collections.singletonList(
                    TapInsertRecordEvent.create().table(tableName).after(record)), sourceTable, ignored -> { });

            try (Connection connection = rootConnection(); Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(
                         "SELECT data->>'payload' FROM public.\"" + tableName + "\"")) {
                assertTrue(resultSet.next());
                assertEquals("\\x000fff", resultSet.getString(1));
            }
        } finally {
            connector.stop(context);
            dropTable(tableName);
        }
    }

    @Test
    void websocketStreamingWritesSupportedScalarTypes() throws Throwable {
        String tableName = "tapdata_ws_types_" + shortSuffix();
        RisingWaveConnector connector = new RisingWaveConnector();
        TapConnectionContext context = connectionContext(
                "public", "streaming", "root", "",
                "ws://127.0.0.1:4560", "");
        try {
            connector.init(context);
            TapTable table = new TapTable(tableName)
                    .add(new TapField("id", "integer").isPrimaryKey(true).primaryKeyPos(1))
                    .add(new TapField("small_value", "smallint"))
                    .add(new TapField("big_value", "bigint"))
                    .add(new TapField("real_value", "real"))
                    .add(new TapField("double_value", "double precision"))
                    .add(new TapField("numeric_value", "numeric"))
                    .add(new TapField("bool_value", "boolean"))
                    .add(new TapField("text_value", "varchar"))
                    .add(new TapField("date_value", "date"))
                    .add(new TapField("time_value", "time"))
                    .add(new TapField("timestamp_value", "timestamp"))
                    .add(new TapField("timestamptz_value", "timestamp with time zone"))
                    .add(new TapField("binary_value", "bytea"))
                    .add(new TapField("json_value", "jsonb"))
                    .add(new TapField("nullable_value", "text"));
            connector.createTable(null, new TapCreateTableEvent().table(table));

            Map<String, Object> record = new LinkedHashMap<>();
            record.put("id", 7);
            record.put("small_value", 12);
            record.put("big_value", 9007199254740993L);
            record.put("real_value", 1.25f);
            record.put("double_value", 2.5d);
            record.put("numeric_value", new BigDecimal("1234567890.123456789"));
            record.put("bool_value", true);
            record.put("text_value", "TapData 中文 🚀");
            record.put("date_value", "2026-07-14");
            record.put("time_value", "15:16:17.123456");
            record.put("timestamp_value", "2026-07-14 15:16:17.123456");
            record.put("timestamptz_value", "2026-07-14T15:16:17.123456+02:00");
            record.put("binary_value", new byte[]{0x00, 0x0f, (byte) 0xff});
            record.put("json_value", Collections.singletonMap("nested", java.util.Arrays.asList(1, "two")));
            record.put("nullable_value", null);

            ConnectorFunctions functions = new ConnectorFunctions();
            connector.registerCapabilities(functions, new TapCodecsRegistry());
            functions.getWriteRecordFunction().writeRecord(
                    null,
                    Collections.singletonList(TapInsertRecordEvent.create().table(tableName).after(record)),
                    table,
                    ignored -> { });

            try (Connection connection = rootConnection(); Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(
                         "SELECT id, small_value, big_value, real_value, double_value, numeric_value, "
                                 + "bool_value, text_value, date_value, time_value, timestamp_value, "
                                 + "timestamptz_value, binary_value, json_value::varchar, nullable_value "
                                 + "FROM public.\"" + tableName + "\"")) {
                assertTrue(resultSet.next());
                assertEquals(7, resultSet.getInt(1));
                assertEquals(12, resultSet.getInt(2));
                assertEquals(9007199254740993L, resultSet.getLong(3));
                assertEquals(1.25f, resultSet.getFloat(4));
                assertEquals(2.5d, resultSet.getDouble(5));
                assertEquals(new BigDecimal("1234567890.123456789"), resultSet.getBigDecimal(6));
                assertTrue(resultSet.getBoolean(7));
                assertEquals("TapData 中文 🚀", resultSet.getString(8));
                assertEquals("2026-07-14", resultSet.getDate(9).toString());
                assertEquals("15:16:17", resultSet.getTime(10).toString());
                assertEquals("2026-07-14 15:16:17.123456", resultSet.getTimestamp(11).toString());
                // The local test session uses UTC+08, so the +02 input must shift by six hours.
                assertEquals("2026-07-14 21:16:17.123456", resultSet.getTimestamp(12).toString());
                assertArrayEquals(new byte[]{0x00, 0x0f, (byte) 0xff}, resultSet.getBytes(13));
                assertEquals("{\"nested\": [1, \"two\"]}", resultSet.getString(14));
                assertEquals(null, resultSet.getString(15));
                assertTrue(!resultSet.next());
            }
        } finally {
            connector.stop(context);
            dropTable(tableName);
        }
    }

    private static List<TestItem> runConnectionTest(String schema, String mode) throws Throwable {
        return runConnectionTest(schema, mode, "root", "");
    }

    private static List<TestItem> runConnectionTest(
            String schema, String mode, String user, String password) throws Throwable {
        return runConnectionTest(
                schema, mode, user, password, "ws://127.0.0.1:4560", "");
    }

    private static List<TestItem> runConnectionTest(
            String schema, String mode, String user, String password,
            String ingestEndpoint, String webhookSecret) throws Throwable {
        TapConnectionContext context = connectionContext(
                schema, mode, user, password, ingestEndpoint, webhookSecret);
        List<TestItem> items = new ArrayList<>();
        new RisingWaveConnector().connectionTest(context, items::add);
        return items;
    }

    private static TapConnectionContext connectionContext(
            String schema, String mode, String user, String password,
            String ingestEndpoint, String webhookSecret) {
        DataMap config = DataMap.create()
                .kv("host", "127.0.0.1")
                .kv("port", 4566)
                .kv("database", "dev")
                .kv("schema", schema)
                .kv("user", user)
                .kv("password", password)
                .kv("sslmode", "disable")
                .kv("ingest_mode", mode)
                .kv("ingestEndpoint", ingestEndpoint)
                .kv("webhookSecret", webhookSecret);
        return new TapConnectionContext(null, config, DataMap.create(), null);
    }

    private static String shortSuffix() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }

    private static void writeAfterStart(CountDownLatch start,
                                        ConnectorFunctions functions,
                                        TapTable table,
                                        Map<String, Object> row) {
        try {
            start.await(10, TimeUnit.SECONDS);
            functions.getWriteRecordFunction().writeRecord(null, Collections.singletonList(
                    TapInsertRecordEvent.create().table(table.getId()).after(row)),
                    table, ignored -> { });
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    private static void dropTable(String tableName) throws Exception {
        try (Connection connection = rootConnection(); Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS public.\"" + tableName + "\"");
            statement.execute("DROP SECRET IF EXISTS public.\""
                    + RisingWaveWebhookSecret.automaticName("public", tableName) + "\"");
        }
    }

    private static int querySecretCount(String secretName) throws Exception {
        try (Connection connection = rootConnection(); java.sql.PreparedStatement statement =
                     connection.prepareStatement("SELECT count(*) FROM rw_catalog.rw_secrets "
                             + "WHERE name = ?")) {
            statement.setString(1, secretName);
            try (ResultSet resultSet = statement.executeQuery()) {
                assertTrue(resultSet.next());
                return resultSet.getInt(1);
            }
        }
    }

    private static Map<String, Object> record(int id, String name) {
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("id", id);
        record.put("name", name);
        return record;
    }

    private static void await(List<CompletableFuture<Void>> futures) throws Exception {
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(10, TimeUnit.SECONDS);
    }

    private static int queryCount(String tableName) throws Exception {
        try (Connection connection = rootConnection(); Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(
                     "SELECT count(*) FROM public.\"" + tableName + "\"")) {
            assertTrue(resultSet.next());
            return resultSet.getInt(1);
        }
    }

    private static void awaitCount(String tableName, int expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        int actual;
        do {
            actual = queryCount(tableName);
            if (actual == expected) {
                return;
            }
            Thread.sleep(100L);
        } while (System.nanoTime() < deadline);
        assertEquals(expected, actual);
    }

    private static void awaitName(String tableName, int id, String expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        String actual;
        do {
            actual = queryNameOrNull(tableName, id);
            if (expected.equals(actual)) {
                return;
            }
            Thread.sleep(100L);
        } while (System.nanoTime() < deadline);
        assertEquals(expected, actual);
    }

    private static void awaitQuantity(String tableName, int id, Integer expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        Integer actual;
        do {
            actual = queryQuantityOrNull(tableName, id);
            if (java.util.Objects.equals(expected, actual)) {
                return;
            }
            Thread.sleep(100L);
        } while (System.nanoTime() < deadline);
        assertEquals(expected, actual);
    }

    private static void awaitQualifiedName(String schema, String tableName, int id, String expected)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        String actual = null;
        do {
            try (Connection connection = rootConnection();
                 java.sql.PreparedStatement statement = connection.prepareStatement(
                         "SELECT name FROM " + RisingWaveSql.quoteIdentifier(schema) + "."
                                 + RisingWaveSql.quoteIdentifier(tableName) + " WHERE id = ?")) {
                statement.setInt(1, id);
                try (ResultSet resultSet = statement.executeQuery()) {
                    actual = resultSet.next() ? resultSet.getString(1) : null;
                }
            }
            if (expected.equals(actual)) {
                return;
            }
            Thread.sleep(100L);
        } while (System.nanoTime() < deadline);
        assertEquals(expected, actual);
    }

    private static Integer queryQuantityOrNull(String tableName, int id) throws Exception {
        try (Connection connection = rootConnection();
             java.sql.PreparedStatement statement = connection.prepareStatement(
                     "SELECT quantity FROM public.\"" + tableName + "\" WHERE id = ?")) {
            statement.setInt(1, id);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() ? (Integer) resultSet.getObject(1) : null;
            }
        }
    }

    private static int queryTableCount(String tableName) throws Exception {
        try (Connection connection = rootConnection();
             java.sql.PreparedStatement statement = connection.prepareStatement(
                     "SELECT count(*) FROM information_schema.tables "
                             + "WHERE table_schema = 'public' AND table_name = ?")) {
            statement.setString(1, tableName);
            try (ResultSet resultSet = statement.executeQuery()) {
                assertTrue(resultSet.next());
                return resultSet.getInt(1);
            }
        }
    }

    private static String queryName(String tableName, int id) throws Exception {
        String name = queryNameOrNull(tableName, id);
        assertNotNull(name);
        return name;
    }

    private static String queryNameOrNull(String tableName, int id) throws Exception {
        try (Connection connection = rootConnection();
             java.sql.PreparedStatement statement = connection.prepareStatement(
                     "SELECT name FROM public.\"" + tableName + "\" WHERE id = ?")) {
            statement.setInt(1, id);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() ? resultSet.getString(1) : null;
            }
        }
    }

    private static int writeProbeTableCount() throws Exception {
        try (Connection connection = rootConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(
                     "SELECT count(*) FROM information_schema.tables "
                             + "WHERE table_schema = 'public' AND table_name LIKE 'tap___test_%'")) {
            assertTrue(resultSet.next());
            return resultSet.getInt(1);
        }
    }

    private static Connection rootConnection() throws Exception {
        return DriverManager.getConnection(
                "jdbc:postgresql://127.0.0.1:4566/dev?sslmode=disable", "root", "");
    }

    private static void assertSuccessful(List<TestItem> items, String name) {
        TestItem item = find(items, name);
        assertEquals(TestItem.RESULT_SUCCESSFULLY, item.getResult(), item::toString);
    }

    private static void assertFailed(List<TestItem> items, String name) {
        TestItem item = find(items, name);
        assertEquals(TestItem.RESULT_FAILED, item.getResult(), item::toString);
    }

    private static TestItem find(List<TestItem> items, String name) {
        TestItem item = items.stream()
                .filter(candidate -> name.equals(candidate.getItem()))
                .findFirst()
                .orElse(null);
        assertNotNull(item, () -> "Missing test item " + name + " in " + items);
        return item;
    }
}
