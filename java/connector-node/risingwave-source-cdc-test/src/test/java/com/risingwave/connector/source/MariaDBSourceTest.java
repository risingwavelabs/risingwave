/*
 * Copyright 2026 RisingWave Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.risingwave.connector.source;

import static org.assertj.core.api.Assertions.assertThat;

import com.risingwave.connector.ConnectorServiceImpl;
import com.risingwave.proto.ConnectorServiceProto.CdcMessage;
import com.risingwave.proto.ConnectorServiceProto.GetEventStreamResponse;
import com.risingwave.proto.ConnectorServiceProto.SourceType;
import com.risingwave.proto.ConnectorServiceProto.TableSchema;
import com.risingwave.proto.Data;
import com.risingwave.proto.PlanCommon;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.management.ObjectName;
import javax.sql.DataSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.MariaDBContainer;

public class MariaDBSourceTest {
    private static final MariaDBContainer<?> MARIADB =
            new MariaDBContainer<>("mariadb:11.4")
                    .withDatabaseName("test")
                    .withUsername("root")
                    .withPassword("test")
                    .withCommand(
                            "--log-bin=mariadb-bin",
                            "--binlog-format=ROW",
                            "--binlog-row-image=FULL",
                            "--server-id=4201",
                            "--binlog-legacy-event-pos=ON");

    private static final Server CONNECTOR_SERVER =
            ServerBuilder.forPort(SourceTestClient.DEFAULT_PORT)
                    .addService(new ConnectorServiceImpl())
                    .build();
    private static DataSource dataSource;
    private static ManagedChannel connectorChannel;
    private static SourceTestClient testClient;

    @BeforeClass
    public static void init() throws Exception {
        com.risingwave.java.binding.Binding.initObjectStoreForTest(
                "hummock+memory", "mariadb-integration-test-data");
        CONNECTOR_SERVER.start();
        MARIADB.start();
        restartClient();
        dataSource =
                SourceTestClient.getDataSource(
                        MARIADB.getJdbcUrl(),
                        MARIADB.getUsername(),
                        MARIADB.getPassword(),
                        MARIADB.getDriverClassName());
    }

    @AfterClass
    public static void cleanup() {
        if (connectorChannel != null) {
            connectorChannel.shutdownNow();
        }
        CONNECTOR_SERVER.shutdownNow();
        MARIADB.stop();
    }

    @Test
    public void validatesAndStreamsWithDedicatedMariaDbConnector() throws Exception {
        try (Connection connection = SourceTestClient.connect(dataSource)) {
            SourceTestClient.performQuery(
                    connection,
                    "CREATE TABLE orders (id INT PRIMARY KEY, customer VARCHAR(50) NOT NULL)");

            TableSchema schema =
                    TableSchema.newBuilder()
                            .addColumns(column("id", Data.DataType.TypeName.INT32))
                            .addColumns(column("customer", Data.DataType.TypeName.VARCHAR))
                            .addPkIndices(0)
                            .build();
            var validation =
                    testClient.validateSource(
                            MARIADB.getJdbcUrl(),
                            MARIADB.getHost(),
                            MARIADB.getUsername(),
                            MARIADB.getPassword(),
                            SourceType.MARIADB,
                            schema,
                            "test",
                            "orders");
            assertThat(validation.getError().getErrorMessage()).isEmpty();

            SourceTestClient.performQuery(connection, "INSERT INTO orders VALUES (1, 'snapshot')");

            Map<String, String> properties = sourceProperties();
            Iterator<GetEventStreamResponse> stream =
                    testClient.getEventStream(SourceType.MARIADB, 1005, properties);
            CountDownLatch snapshotSeen = new CountDownLatch(1);
            ExecutorService reader = Executors.newSingleThreadExecutor();
            Future<StreamResult> changes =
                    reader.submit(() -> readSnapshotAndTransaction(stream, snapshotSeen));

            assertThat(snapshotSeen.await(30, TimeUnit.SECONDS)).isTrue();
            try (Statement statement = connection.createStatement()) {
                connection.setAutoCommit(false);
                statement.executeUpdate("INSERT INTO orders VALUES (2, 'inserted')");
                statement.executeUpdate("UPDATE orders SET customer = 'updated' WHERE id = 1");
                statement.executeUpdate("DELETE FROM orders WHERE id = 2");
                connection.commit();
                connection.setAutoCommit(true);
            }

            StreamResult result = changes.get(30, TimeUnit.SECONDS);
            assertThat(result.operations()).containsExactlyInAnyOrder("r", "c", "u", "d");
            assertThat(result.transactionStatuses()).contains("BEGIN", "END");
            assertThat(result.checkpoint()).isNotBlank();

            connectorChannel.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
            awaitMariaDbConnectorStopped();
            SourceTestClient.performQuery(connection, "INSERT INTO orders VALUES (3, 'resumed')");
            restartClient();

            Iterator<GetEventStreamResponse> resumedStream =
                    testClient.getEventStream(
                            SourceType.MARIADB, 1005, properties, result.checkpoint(), true);
            Future<CdcMessage> resumed = reader.submit(() -> readResumedInsert(resumedStream));
            CdcMessage resumedInsert = resumed.get(30, TimeUnit.SECONDS);
            assertThat(resumedInsert.getPayload()).contains("\"id\":3", "\"op\":\"c\"");
            assertThat(resumedInsert.getOffset()).isNotBlank();
            reader.shutdownNow();
        }
    }

    private static StreamResult readSnapshotAndTransaction(
            Iterator<GetEventStreamResponse> stream, CountDownLatch snapshotSeen) {
        var operations = new java.util.HashSet<String>();
        var transactionStatuses = new java.util.HashSet<String>();
        String checkpoint = "";
        while (stream.hasNext()) {
            for (CdcMessage message : stream.next().getEventsList()) {
                assertMariaDbMessage(message);
                if (message.getMsgType() == CdcMessage.CdcMessageType.DATA) {
                    String payload = message.getPayload();
                    for (String operation : Set.of("r", "c", "u", "d")) {
                        if (payload.contains("\"op\":\"" + operation + "\"")) {
                            operations.add(operation);
                            checkpoint = message.getOffset();
                            if (operation.equals("r")) {
                                snapshotSeen.countDown();
                            }
                        }
                    }
                } else if (message.getMsgType() == CdcMessage.CdcMessageType.TRANSACTION_META) {
                    for (String status : Set.of("BEGIN", "END")) {
                        if (message.getPayload().contains("\"status\":\"" + status + "\"")) {
                            transactionStatuses.add(status);
                        }
                    }
                }
            }
            if (operations.containsAll(Set.of("r", "c", "u", "d"))
                    && transactionStatuses.containsAll(Set.of("BEGIN", "END"))) {
                return new StreamResult(operations, transactionStatuses, checkpoint);
            }
        }
        throw new AssertionError("MariaDB event stream ended before all changes were observed");
    }

    private static CdcMessage readResumedInsert(Iterator<GetEventStreamResponse> stream) {
        while (stream.hasNext()) {
            for (CdcMessage message : stream.next().getEventsList()) {
                assertMariaDbMessage(message);
                if (message.getMsgType() == CdcMessage.CdcMessageType.DATA
                        && message.getPayload().contains("\"id\":3")) {
                    return message;
                }
            }
        }
        throw new AssertionError("MariaDB resumed stream ended before the new insert was observed");
    }

    private static void assertMariaDbMessage(CdcMessage message) {
        assertThat(message.getSourceType()).isEqualTo(SourceType.MARIADB);
        if (!message.getPayload().isBlank()
                && message.getMsgType() == CdcMessage.CdcMessageType.DATA) {
            assertThat(message.getPayload()).contains("\"connector\":\"mariadb\"");
        }
    }

    private static Map<String, String> sourceProperties() {
        var properties = new HashMap<String, String>();
        properties.put("hostname", MARIADB.getHost());
        properties.put("port", Integer.toString(MARIADB.getMappedPort(3306)));
        properties.put("username", MARIADB.getUsername());
        properties.put("password", MARIADB.getPassword());
        properties.put("database.name", "test");
        properties.put("table.name", "orders");
        properties.put("server.id", "4102");
        properties.put("transactional", "true");
        properties.put("debezium.poll.interval.ms", "100");
        return properties;
    }

    private static void restartClient() {
        connectorChannel =
                Grpc.newChannelBuilder(
                                "localhost:" + SourceTestClient.DEFAULT_PORT,
                                InsecureChannelCredentials.create())
                        .build();
        testClient = new SourceTestClient(connectorChannel);
    }

    private static void awaitMariaDbConnectorStopped() throws Exception {
        var mBeanServer = ManagementFactory.getPlatformMBeanServer();
        var mariaDbMetrics =
                new ObjectName(
                        "debezium.mariadb:type=connector-metrics,context=*,server=RW_CDC_1005");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!mBeanServer.queryNames(mariaDbMetrics, null).isEmpty()) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("MariaDB connector did not stop within 10 seconds");
            }
            Thread.sleep(50);
        }
    }

    private record StreamResult(
            Set<String> operations, Set<String> transactionStatuses, String checkpoint) {}

    private static PlanCommon.ColumnDesc column(String name, Data.DataType.TypeName typeName) {
        return PlanCommon.ColumnDesc.newBuilder()
                .setName(name)
                .setColumnType(Data.DataType.newBuilder().setTypeName(typeName).build())
                .build();
    }
}
