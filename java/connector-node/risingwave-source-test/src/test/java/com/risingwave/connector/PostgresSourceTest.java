// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.risingwave.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.Data;
import io.grpc.*;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import javax.sql.DataSource;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

public class PostgresSourceTest {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresSourceTest.class.getName());

    private static final PostgreSQLContainer<?> pg =
            new PostgreSQLContainer<>("postgres:15-alpine")
                    .withDatabaseName("test")
                    .withUsername("postgres")
                    .withCommand("postgres -c wal_level=logical -c max_wal_senders=10");

    public static Server connectorServer =
            ServerBuilder.forPort(ConnectorService.DEFAULT_PORT)
                    .addService(new ConnectorServiceImpl())
                    .build();

    public static SourceTestClient testClient =
            new SourceTestClient(
                    Grpc.newChannelBuilder(
                                    "localhost:" + ConnectorService.DEFAULT_PORT,
                                    InsecureChannelCredentials.create())
                            .build());

    private static DataSource pgDataSource;

    @BeforeClass
    public static void init() {
        // generate orders.tbl test data
        SourceTestClient.genOrdersTable(10000);
        // start connector server and postgres...
        try {
            connectorServer.start();
            LOG.info("connector service started");
            pg.withCopyFileToContainer(
                    MountableFile.forClasspathResource("orders.tbl"), "/home/orders.tbl");
            pg.start();
            pg.withUsername("postgres")
                    .execInContainer(
                            "sh",
                            "-c",
                            "echo 'host replication postgres 172.17.0.1/32 trust' >> /var/lib/postgresql/data/pg_hba.conf");
            pgDataSource = SourceTestClient.getDataSource(pg);
            LOG.info("postgres started");
        } catch (IOException e) {
            fail("IO exception: ", e);
        } catch (InterruptedException e) {
            fail("Interrupted exception", e);
        }
        // check pg configuration...
        try {
            Connection connection = SourceTestClient.connect(pgDataSource);
            SourceTestClient.performQuery(connection, "SELECT pg_reload_conf()");
            ResultSet resultSet =
                    SourceTestClient.performQuery(
                            connection, testClient.sqlStmts.getProperty("postgres.wal"));
            assertThat(resultSet.getString("wal_level"))
                    .isEqualTo("logical")
                    .as("pg: wal_level logical");
            connection.close();
        } catch (SQLException e) {
            fail("SQL exception: ", e);
        }
    }

    @AfterClass
    public static void cleanup() {
        connectorServer.shutdown();
        pg.stop();
    }

    // create a TPC-H orders table in postgres
    // insert 10,000 rows into orders
    // check if the number of changes debezium captures is 10,000
    @Test
    public void testLines() throws InterruptedException, SQLException {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        Connection connection = SourceTestClient.connect(pgDataSource);
        String query = testClient.sqlStmts.getProperty("tpch.create.orders");
        SourceTestClient.performQuery(connection, query);
        query = "COPY orders FROM '/home/orders.tbl' WITH DELIMITER '|'";
        SourceTestClient.performQuery(connection, query);
        Iterator<ConnectorServiceProto.GetEventStreamResponse> eventStream =
                testClient.getEventStreamStart(
                        pg, ConnectorServiceProto.SourceType.POSTGRES, "test", "orders");
        Callable<Integer> countTask =
                () -> {
                    int count = 0;
                    while (eventStream.hasNext()) {
                        List<ConnectorServiceProto.CdcMessage> messages =
                                eventStream.next().getEventsList();
                        for (ConnectorServiceProto.CdcMessage ignored : messages) {
                            count++;
                        }
                        if (count == 10000) {
                            return count;
                        }
                    }
                    return count;
                };
        Future<Integer> countResult = executorService.submit(countTask);
        try {
            int count = countResult.get();
            LOG.info("number of cdc messages received: {}", count);
            assertEquals(count, 10000);
        } catch (ExecutionException e) {
            fail("Execution exception: ", e);
        }
        connection.close();
    }

    @Test
    public void testPermissionCheck() {
        Connection connection = SourceTestClient.connect(pgDataSource);
        String query =
                "CREATE TABLE IF NOT EXISTS orders (o_key BIGINT NOT NULL, o_val INT, PRIMARY KEY (o_key))";
        SourceTestClient.performQuery(connection, query);
        // create a partial publication, check whether error is reported
        query = "CREATE PUBLICATION dbz_publication FOR TABLE orders (o_key)";
        SourceTestClient.performQuery(connection, query);
        ConnectorServiceProto.TableSchema tableSchema =
                ConnectorServiceProto.TableSchema.newBuilder()
                        .addColumns(
                                ConnectorServiceProto.TableSchema.Column.newBuilder()
                                        .setName("o_key")
                                        .setDataType(Data.DataType.TypeName.INT64)
                                        .build())
                        .addColumns(
                                ConnectorServiceProto.TableSchema.Column.newBuilder()
                                        .setName("o_val")
                                        .setDataType(Data.DataType.TypeName.INT32)
                                        .build())
                        .addPkIndices(0)
                        .build();
        Iterator<ConnectorServiceProto.GetEventStreamResponse> eventStream1 =
                testClient.getEventStreamValidate(
                        pg,
                        ConnectorServiceProto.SourceType.POSTGRES,
                        tableSchema,
                        "test",
                        "orders");
        StatusRuntimeException exception1 =
                assertThrows(
                        StatusRuntimeException.class,
                        () -> {
                            eventStream1.hasNext();
                        });
        assertEquals(
                exception1.getMessage(),
                "INVALID_ARGUMENT: INTERNAL: The publication 'dbz_publication' does not cover all necessary columns in table orders");
        // revoke superuser and replication, check if reports error
        query = "ALTER USER " + pg.getUsername() + " nosuperuser noreplication";
        SourceTestClient.performQuery(connection, query);
        Iterator<ConnectorServiceProto.GetEventStreamResponse> eventStream2 =
                testClient.getEventStreamValidate(
                        pg,
                        ConnectorServiceProto.SourceType.POSTGRES,
                        tableSchema,
                        "test",
                        "orders");
        StatusRuntimeException exception2 =
                assertThrows(
                        StatusRuntimeException.class,
                        () -> {
                            eventStream2.hasNext();
                        });
        assertEquals(
                exception2.getMessage(),
                "INVALID_ARGUMENT: INTERNAL: Postgres user must be superuser or replication role to start walsender.");
    }

    // generates test cases for the risingwave debezium parser
    @Ignore
    @Test
    public void getTestJson() throws InterruptedException, SQLException {
        Connection connection = SourceTestClient.connect(pgDataSource);
        String query =
                "CREATE TABLE IF NOT EXISTS orders ("
                        + "O_KEY BIGINT NOT NULL, "
                        + "O_BOOL BOOLEAN, "
                        + "O_BITS BIT(3), "
                        + "O_TINY SMALLINT, "
                        + "O_INT INT, "
                        + "O_REAL REAL, "
                        + "O_DOUBLE DOUBLE PRECISION, "
                        + "O_DECIMAL DECIMAL(15, 2), "
                        + "O_CHAR CHAR(15), "
                        + "O_DATE DATE, "
                        + "O_TIME TIME, "
                        + "O_TIMESTAMP TIMESTAMP, "
                        + "O_JSON JSON, "
                        + "O_TEXT_ARR TEXT[][], "
                        + "PRIMARY KEY (O_KEY))";
        SourceTestClient.performQuery(connection, query);
        Iterator<ConnectorServiceProto.GetEventStreamResponse> eventStream =
                testClient.getEventStreamStart(
                        pg, ConnectorServiceProto.SourceType.POSTGRES, "test", "orders");
        Thread t1 =
                new Thread(
                        () -> {
                            while (eventStream.hasNext()) {
                                List<ConnectorServiceProto.CdcMessage> messages =
                                        eventStream.next().getEventsList();
                                for (ConnectorServiceProto.CdcMessage msg : messages) {
                                    LOG.info("{}", msg.getPayload());
                                }
                            }
                        });
        // Q1: ordinary insert (read)
        Thread.sleep(1000);
        t1.start();
        query =
                "INSERT INTO orders (O_KEY, O_BOOL, O_BITS, O_TINY, O_INT, O_REAL, O_DOUBLE, O_DECIMAL, O_CHAR, O_DATE, O_TIME, O_TIMESTAMP, O_JSON, O_TEXT_ARR)"
                        + "VALUES(111, TRUE, b'111', -1, -1111, -11.11, -111.11111, -111.11, 'yes please', '2011-11-11', '11:11:11', '2011-11-11 11:11:11.123456', '{\"k1\": \"v1\", \"k2\": 11}', ARRAY[['meeting', 'lunch'], ['training', 'presentation']])";
        SourceTestClient.performQuery(connection, query);
        Thread.sleep(1000);
        connection.close();
    }
}
