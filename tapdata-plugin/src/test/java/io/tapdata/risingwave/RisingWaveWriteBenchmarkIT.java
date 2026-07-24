package io.tapdata.risingwave;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Manual local benchmark; excluded unless both benchmark properties are enabled. */
class RisingWaveWriteBenchmarkIT {

    @Test
    void comparesWebSocketAndJdbcInsertThroughput() throws Throwable {
        assumeTrue(Boolean.getBoolean("risingwave.it"));
        assumeTrue(Boolean.getBoolean("risingwave.benchmark"));

        int rows = Integer.getInteger("risingwave.benchmark.rows", 2000);
        int batchSize = Integer.getInteger("risingwave.benchmark.batchSize", 500);
        BenchmarkResult streaming = benchmark("streaming", rows, batchSize);
        BenchmarkResult jdbc = benchmark("jdbc", rows, batchSize);

        System.out.printf("BENCHMARK mode=streaming rows=%d batch=%d elapsed_ms=%d rows_per_second=%.1f%n",
                rows, batchSize, streaming.elapsedMillis, streaming.rowsPerSecond());
        System.out.printf("BENCHMARK mode=streaming visibility_lag_ms=%d%n", streaming.visibilityLagMillis);
        System.out.printf("BENCHMARK mode=jdbc rows=%d batch=%d elapsed_ms=%d rows_per_second=%.1f%n",
                rows, batchSize, jdbc.elapsedMillis, jdbc.rowsPerSecond());
        System.out.printf("BENCHMARK mode=jdbc visibility_lag_ms=%d%n", jdbc.visibilityLagMillis);
        System.out.printf("BENCHMARK websocket_speedup=%.2fx%n",
                streaming.rowsPerSecond() / jdbc.rowsPerSecond());
    }

    private BenchmarkResult benchmark(String mode, int rows, int batchSize) throws Throwable {
        String tableName = "tapdata_bench_" + mode + "_"
                + UUID.randomUUID().toString().replace("-", "").substring(0, 10);
        RisingWaveConnector connector = new RisingWaveConnector();
        TapConnectionContext context = context(mode);
        try {
            connector.init(context);
            TapTable table = new TapTable(tableName)
                    .add(new TapField("id", "integer").isPrimaryKey(true).primaryKeyPos(1))
                    .add(new TapField("payload", "varchar"));
            connector.createTable(null, new TapCreateTableEvent().table(table));
            ConnectorFunctions functions = new ConnectorFunctions();
            connector.registerCapabilities(functions, new TapCodecsRegistry());

            long started = System.nanoTime();
            for (int offset = 0; offset < rows; offset += batchSize) {
                List<TapRecordEvent> events = new ArrayList<>();
                int end = Math.min(rows, offset + batchSize);
                for (int id = offset; id < end; id++) {
                    Map<String, Object> record = new LinkedHashMap<>();
                    record.put("id", id);
                    record.put("payload", "benchmark-payload-" + id);
                    events.add(TapInsertRecordEvent.create().table(tableName).after(record));
                }
                functions.getWriteRecordFunction().writeRecord(null, events, table, ignored -> { });
            }
            long elapsedMillis = Math.max(1L, (System.nanoTime() - started) / 1_000_000L);

            long visibilityStarted = System.nanoTime();
            int visibleRows = -1;
            try (Connection connection = rootConnection(); Statement statement = connection.createStatement()) {
                long deadline = System.nanoTime() + 30_000_000_000L;
                do {
                    try (ResultSet resultSet = statement.executeQuery(
                            "SELECT count(*) FROM public.\"" + tableName + "\"")) {
                        resultSet.next();
                        visibleRows = resultSet.getInt(1);
                    }
                    if (visibleRows == rows) {
                        break;
                    }
                    Thread.sleep(100L);
                } while (System.nanoTime() < deadline);
            }
            assertEquals(rows, visibleRows);
            long visibilityLagMillis = (System.nanoTime() - visibilityStarted) / 1_000_000L;
            return new BenchmarkResult(rows, elapsedMillis, visibilityLagMillis);
        } finally {
            connector.stop(context);
            try (Connection connection = rootConnection(); Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE IF EXISTS public.\"" + tableName + "\"");
            }
        }
    }

    private static TapConnectionContext context(String mode) {
        DataMap config = DataMap.create()
                .kv("host", "127.0.0.1")
                .kv("port", 4566)
                .kv("database", "dev")
                .kv("schema", "public")
                .kv("user", "root")
                .kv("password", "")
                .kv("sslmode", "disable")
                .kv("ingest_mode", mode)
                .kv("ingestEndpoint", "ws://127.0.0.1:4560")
                .kv("webhookSecret", "");
        return new TapConnectionContext(null, config, DataMap.create(), null);
    }

    private static Connection rootConnection() throws Exception {
        return DriverManager.getConnection(
                "jdbc:postgresql://127.0.0.1:4566/dev?sslmode=disable", "root", "");
    }

    private static final class BenchmarkResult {
        private final int rows;
        private final long elapsedMillis;
        private final long visibilityLagMillis;

        private BenchmarkResult(int rows, long elapsedMillis, long visibilityLagMillis) {
            this.rows = rows;
            this.elapsedMillis = elapsedMillis;
            this.visibilityLagMillis = visibilityLagMillis;
        }

        private double rowsPerSecond() {
            return rows * 1000.0 / elapsedMillis;
        }
    }
}
