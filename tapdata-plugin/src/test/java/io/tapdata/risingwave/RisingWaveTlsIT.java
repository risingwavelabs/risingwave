package io.tapdata.risingwave;

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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Manual WSS integration test using the TLS-enabled local ingest proxy. */
class RisingWaveTlsIT {

    @Test
    void writesThroughTrustedWssEndpoint() throws Exception {
        assumeTrue(Boolean.getBoolean("risingwave.it"));
        String endpoint = System.getProperty("risingwave.tlsEndpoint");
        assumeTrue(endpoint != null && !endpoint.isEmpty());

        String table = "tapdata_wss_"
                + UUID.randomUUID().toString().replace("-", "").substring(0, 10);
        try (Connection connection = rootConnection(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE public.\"" + table
                    + "\" (id integer, payload varchar, PRIMARY KEY (id)) WITH (connector='webhook')");
        }

        Map<String, Object> record = new LinkedHashMap<>();
        record.put("id", 1);
        record.put("payload", "trusted-wss");
        try (WsIngestClient client = new WsIngestClient(endpoint, "dev", "public", table, "")) {
            client.connect();
            await(client.sendBatch(Collections.singletonList(
                    new WsIngestClient.DmlOperation("insert", null, record))));
            awaitCount(table, 1);
        } finally {
            try (Connection connection = rootConnection(); Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE IF EXISTS public.\"" + table + "\"");
            }
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
}
