package io.tapdata.risingwave;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.risingwave.streaming.WsIngestClient;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Manual fault-injection test; requires scripts/ws_ack_drop_proxy.py. */
class RisingWaveAckLossIT {

    @Test
    void replayAfterPersistedButLostAckMatchesModeSemantics() throws Exception {
        assumeTrue(Boolean.getBoolean("risingwave.it"));
        String proxyEndpoint = System.getProperty("risingwave.ackLossProxyEndpoint");
        assumeTrue(proxyEndpoint != null && !proxyEndpoint.isEmpty());

        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 10);
        String keyedTable = "tapdata_ack_keyed_" + suffix;
        String jsonbTable = "tapdata_ack_jsonb_" + suffix;
        try (Connection connection = rootConnection(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE public.\"" + keyedTable
                    + "\" (id integer, payload varchar, PRIMARY KEY (id)) WITH (connector='webhook')");
            statement.execute("CREATE TABLE public.\"" + jsonbTable
                    + "\" (data jsonb) WITH (connector='webhook')");
        }

        Map<String, Object> record = new LinkedHashMap<>();
        record.put("id", 1);
        record.put("payload", "persisted-before-ack");
        try {
            sendAndLoseAck(proxyEndpoint, keyedTable, record);
            awaitCount(keyedTable, 1);
            sendSuccessfully("ws://127.0.0.1:4560", keyedTable, record);
            awaitCount(keyedTable, 1);

            sendAndLoseAck(proxyEndpoint, jsonbTable, record);
            awaitCount(jsonbTable, 1);
            sendSuccessfully("ws://127.0.0.1:4560", jsonbTable, record);
            awaitCount(jsonbTable, 2);
        } finally {
            try (Connection connection = rootConnection(); Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE IF EXISTS public.\"" + keyedTable + "\"");
                statement.execute("DROP TABLE IF EXISTS public.\"" + jsonbTable + "\"");
            }
        }
    }

    @Test
    void connectorRecreatesWebSocketClientAcrossRepeatedLostAcks() throws Throwable {
        assumeTrue(Boolean.getBoolean("risingwave.it"));
        String proxyEndpoint = System.getProperty("risingwave.ackLossProxyEndpoint");
        String countProperty = System.getProperty("risingwave.ackLossReconnectCount");
        assumeTrue(proxyEndpoint != null && !proxyEndpoint.isEmpty());
        assumeTrue(countProperty != null && !countProperty.isEmpty(),
                "Set -Drisingwave.ackLossReconnectCount to match the proxy's --drop-ack-count");
        int reconnectCount = Integer.parseInt(countProperty);

        String tableName = "tapdata_ack_reconnect_"
                + UUID.randomUUID().toString().replace("-", "").substring(0, 10);
        try (Connection connection = rootConnection(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE public.\"" + tableName
                    + "\" (id integer, payload varchar, PRIMARY KEY (id)) WITH (connector='webhook')");
        }

        RisingWaveConnector connector = new RisingWaveConnector();
        TapConnectionContext context = streamingContext(proxyEndpoint);
        TapTable table = new TapTable(tableName)
                .add(new TapField("id", "integer").isPrimaryKey(true).primaryKeyPos(1))
                .add(new TapField("payload", "varchar"));
        try {
            connector.init(context);
            ConnectorFunctions functions = new ConnectorFunctions();
            connector.registerCapabilities(functions, new TapCodecsRegistry());

            for (int id = 1; id <= reconnectCount; id++) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("id", id);
                row.put("payload", "reconnect-" + id);
                assertThrows(RuntimeException.class, () -> functions.getWriteRecordFunction().writeRecord(
                        null, Collections.singletonList(TapInsertRecordEvent.create().table(tableName).after(row)),
                        table, ignored -> { }));
            }
            awaitCount(tableName, reconnectCount);

            for (int id = 1; id <= reconnectCount; id++) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("id", id);
                row.put("payload", "reconnect-" + id);
                functions.getWriteRecordFunction().writeRecord(
                        null, Collections.singletonList(TapInsertRecordEvent.create().table(tableName).after(row)),
                        table, ignored -> { });
            }
            awaitCount(tableName, reconnectCount);
        } finally {
            connector.stop(context);
            try (Connection connection = rootConnection(); Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE IF EXISTS public.\"" + tableName + "\"");
            }
        }
    }

    private static void sendAndLoseAck(
            String endpoint, String table, Map<String, Object> record) throws Exception {
        try (WsIngestClient client = new WsIngestClient(endpoint, "dev", "public", table, "")) {
            client.connect();
            List<CompletableFuture<Void>> futures = client.sendBatch(Collections.singletonList(
                    new WsIngestClient.DmlOperation("insert", null, record)));
            assertThrows(Exception.class, () -> await(futures));
        }
    }

    private static void sendSuccessfully(
            String endpoint, String table, Map<String, Object> record) throws Exception {
        try (WsIngestClient client = new WsIngestClient(endpoint, "dev", "public", table, "")) {
            client.connect();
            await(client.sendBatch(Collections.singletonList(
                    new WsIngestClient.DmlOperation("insert", null, record))));
        }
    }

    private static void awaitCount(String table, int expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        int actual = -1;
        do {
            try (Connection connection = rootConnection(); Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(
                         "SELECT count(*) FROM public.\"" + table + "\"")) {
                resultSet.next();
                actual = resultSet.getInt(1);
            }
            if (actual == expected) {
                return;
            }
            Thread.sleep(100L);
        } while (System.nanoTime() < deadline);
        assertEquals(expected, actual);
    }

    private static void await(List<CompletableFuture<Void>> futures) throws Exception {
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .get(10, TimeUnit.SECONDS);
    }

    private static Connection rootConnection() throws Exception {
        return DriverManager.getConnection(
                "jdbc:postgresql://127.0.0.1:4566/dev?sslmode=disable", "root", "");
    }

    private static TapConnectionContext streamingContext(String ingestEndpoint) {
        DataMap config = DataMap.create()
                .kv("host", "127.0.0.1")
                .kv("port", 4566)
                .kv("database", "dev")
                .kv("schema", "public")
                .kv("user", "root")
                .kv("password", "")
                .kv("sslmode", "disable")
                .kv("ingest_mode", "streaming")
                .kv("ingestEndpoint", ingestEndpoint)
                .kv("webhookSecret", "");
        return new TapConnectionContext(null, config, DataMap.create(), null);
    }
}
