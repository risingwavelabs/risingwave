// Copyright 2025 RisingWave Labs
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

package com.risingwave.connector.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.Assert.*;

import com.risingwave.connector.ConnectorServiceImpl;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.Data;
import com.risingwave.proto.PlanCommon;
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
            ServerBuilder.forPort(SourceTestClient.DEFAULT_PORT)
                    .addService(new ConnectorServiceImpl())
                    .build();

    public static SourceTestClient testClient =
            new SourceTestClient(
                    Grpc.newChannelBuilder(
                                    "localhost:" + SourceTestClient.DEFAULT_PORT,
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
            pgDataSource =
                    SourceTestClient.getDataSource(
                            pg.getJdbcUrl(),
                            pg.getUsername(),
                            pg.getPassword(),
                            pg.getDriverClassName());
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
    public void testLines() throws Exception {
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
                        for (ConnectorServiceProto.CdcMessage msg : messages) {
                            if (!msg.getPayload().isBlank()) {
                                count++;
                            }
                        }
                        if (count >= 10000) {
                            return count;
                        }
                    }
                    return count;
                };
        Future<Integer> countResult = executorService.submit(countTask);
        int count = countResult.get();
        LOG.info("number of cdc messages received: {}", count);
        try {
            // 10000 rows plus one heartbeat message
            assertTrue(count >= 10000);
        } catch (Exception e) {
            Assert.fail("validate rpc fail: " + e.getMessage());
        } finally {
            // cleanup
            query = testClient.sqlStmts.getProperty("tpch.drop.orders");
            SourceTestClient.performQuery(connection, query);
            connection.close();
        }
    }

    // test whether validation catches permission errors
    @Test
    public void testPermissionCheck() throws SQLException {
        // user Postgres creates a superuser debezium
        Connection connPg = SourceTestClient.connect(pgDataSource);
        String query = "CREATE USER debezium";
        SourceTestClient.performQuery(connPg, query);
        query = "ALTER USER debezium SUPERUSER REPLICATION";
        SourceTestClient.performQuery(connPg, query);
        query = "ALTER USER debezium WITH PASSWORD '" + pg.getPassword() + "'";
        SourceTestClient.performQuery(connPg, query);
        // user debezium connects to Postgres
        DataSource dbzDataSource =
                SourceTestClient.getDataSource(
                        pg.getJdbcUrl(), "debezium", pg.getPassword(), pg.getDriverClassName());
        Connection connDbz = SourceTestClient.connect(dbzDataSource);
        query =
                "CREATE TABLE IF NOT EXISTS orders (o_key BIGINT NOT NULL, o_val INT, PRIMARY KEY (o_key))";
        SourceTestClient.performQuery(connDbz, query);
        // create a partial publication, check whether error is reported
        query =
                "CREATE PUBLICATION rw_publication FOR TABLE orders (o_key) WITH ( publish_via_partition_root = true );";
        SourceTestClient.performQuery(connDbz, query);
        ConnectorServiceProto.TableSchema tableSchema =
                ConnectorServiceProto.TableSchema.newBuilder()
                        .addColumns(
                                PlanCommon.ColumnDesc.newBuilder()
                                        .setName("o_key")
                                        .setColumnType(
                                                Data.DataType.newBuilder()
                                                        .setTypeName(Data.DataType.TypeName.INT64)
                                                        .build())
                                        .build())
                        .addColumns(
                                PlanCommon.ColumnDesc.newBuilder()
                                        .setName("o_val")
                                        .setColumnType(
                                                Data.DataType.newBuilder()
                                                        .setTypeName(Data.DataType.TypeName.INT32)
                                                        .build())
                                        .build())
                        .addPkIndices(0)
                        .build();

        try {
            var resp =
                    testClient.validateSource(
                            pg.getJdbcUrl(),
                            pg.getHost(),
                            "debezium",
                            pg.getPassword(),
                            ConnectorServiceProto.SourceType.POSTGRES,
                            tableSchema,
                            "test",
                            "orders");
            assertEquals(
                    "INVALID_ARGUMENT: The publication 'rw_publication' does not cover all columns of the table 'public.orders'",
                    resp.getError().getErrorMessage());
            query = "DROP PUBLICATION dbz_publication";
            SourceTestClient.performQuery(connDbz, query);
            // revoke superuser and replication, check if reports error
            query = "ALTER USER debezium nosuperuser noreplication";
            SourceTestClient.performQuery(connDbz, query);

            resp =
                    testClient.validateSource(
                            pg.getJdbcUrl(),
                            pg.getHost(),
                            "debezium",
                            pg.getPassword(),
                            ConnectorServiceProto.SourceType.POSTGRES,
                            tableSchema,
                            "test",
                            "orders");

            assertEquals(
                    "INVALID_ARGUMENT: Postgres user must be superuser or replication role to start walsender.",
                    resp.getError().getErrorMessage());
        } catch (Exception e) {
            Assert.fail("validate rpc fail: " + e.getMessage());
        } finally {
            // cleanup
            query = testClient.sqlStmts.getProperty("tpch.drop.orders");
            SourceTestClient.performQuery(connDbz, query);
            query = "DROP USER debezium";
            SourceTestClient.performQuery(connPg, query);
            connDbz.close();
            connPg.close();
        }
    }

    @Test
    public void testUserPermissionCheck() throws SQLException {
        Connection connPg = SourceTestClient.connect(pgDataSource);
        // use rds_replication and rds_superuser to simulate the RDS env
        String query = "CREATE ROLE rds_replication";
        SourceTestClient.performQuery(connPg, query);
        query = "CREATE ROLE rds_superuser";
        SourceTestClient.performQuery(connPg, query);
        // user Postgres creates a superuser debezium
        query = "CREATE USER debezium";
        SourceTestClient.performQuery(connPg, query);
        query = "ALTER USER debezium WITH PASSWORD '" + pg.getPassword() + "'";
        SourceTestClient.performQuery(connPg, query);
        query =
                "CREATE TABLE IF NOT EXISTS orders (o_key BIGINT NOT NULL, o_val INT, PRIMARY KEY (o_key))";
        SourceTestClient.performQuery(connPg, query);

        // user debezium connects to Postgres
        DataSource dbzDataSource =
                SourceTestClient.getDataSource(
                        pg.getJdbcUrl(), "debezium", pg.getPassword(), pg.getDriverClassName());
        Connection connDbz = SourceTestClient.connect(dbzDataSource);

        ConnectorServiceProto.TableSchema tableSchema =
                ConnectorServiceProto.TableSchema.newBuilder()
                        .addColumns(
                                PlanCommon.ColumnDesc.newBuilder()
                                        .setName("o_key")
                                        .setColumnType(
                                                Data.DataType.newBuilder()
                                                        .setTypeName(Data.DataType.TypeName.INT64)
                                                        .build())
                                        .build())
                        .addColumns(
                                PlanCommon.ColumnDesc.newBuilder()
                                        .setName("o_val")
                                        .setColumnType(
                                                Data.DataType.newBuilder()
                                                        .setTypeName(Data.DataType.TypeName.INT32)
                                                        .build())
                                        .build())
                        .addPkIndices(0)
                        .build();

        try {
            var resp =
                    testClient.validateSource(
                            pg.getJdbcUrl(),
                            pg.getHost(),
                            "debezium",
                            pg.getPassword(),
                            ConnectorServiceProto.SourceType.POSTGRES,
                            tableSchema,
                            "test",
                            "orders");
            assertEquals(
                    "INVALID_ARGUMENT: Postgres user must be superuser or replication role to start walsender.",
                    resp.getError().getErrorMessage());
            query = "ALTER USER debezium REPLICATION";
            SourceTestClient.performQuery(connPg, query);

            resp =
                    testClient.validateSource(
                            pg.getJdbcUrl(),
                            pg.getHost(),
                            "debezium",
                            pg.getPassword(),
                            ConnectorServiceProto.SourceType.POSTGRES,
                            tableSchema,
                            "test",
                            "orders");
            assertEquals(
                    "INVALID_ARGUMENT: Postgres user must have select privilege on table 'public.orders'",
                    resp.getError().getErrorMessage());
            query = "GRANT SELECT ON orders TO debezium";
            SourceTestClient.performQuery(connPg, query);

            resp =
                    testClient.validateSource(
                            pg.getJdbcUrl(),
                            pg.getHost(),
                            "debezium",
                            pg.getPassword(),
                            ConnectorServiceProto.SourceType.POSTGRES,
                            tableSchema,
                            "test",
                            "orders");
            assertEquals(
                    "INVALID_ARGUMENT: Postgres user must have create privilege on database 'test'",
                    resp.getError().getErrorMessage());

            query = "GRANT CREATE ON DATABASE test TO debezium";
            SourceTestClient.performQuery(connPg, query);

            resp =
                    testClient.validateSource(
                            pg.getJdbcUrl(),
                            pg.getHost(),
                            "debezium",
                            pg.getPassword(),
                            ConnectorServiceProto.SourceType.POSTGRES,
                            tableSchema,
                            "test",
                            "orders");

            assertEquals(
                    "INVALID_ARGUMENT: Postgres user must be the owner of table 'orders' to create/alter publication",
                    resp.getError().getErrorMessage());

            query = "ALTER TABLE orders OWNER TO debezium";
            SourceTestClient.performQuery(connPg, query);

            resp =
                    testClient.validateSource(
                            pg.getJdbcUrl(),
                            pg.getHost(),
                            "debezium",
                            pg.getPassword(),
                            ConnectorServiceProto.SourceType.POSTGRES,
                            tableSchema,
                            "test",
                            "orders");

            assertEquals("", resp.getError().getErrorMessage());

            resp =
                    testClient.validateSource(
                            pg.getJdbcUrl(),
                            pg.getHost(),
                            "debezium",
                            pg.getPassword(),
                            ConnectorServiceProto.SourceType.POSTGRES,
                            tableSchema,
                            "test",
                            "orders",
                            true);
            assertEquals(
                    "INVALID_ARGUMENT: Postgres user must be superuser or replication role to start walsender.",
                    resp.getError().getErrorMessage());
            query = "GRANT rds_replication TO debezium";
            SourceTestClient.performQuery(connPg, query);

            resp =
                    testClient.validateSource(
                            pg.getJdbcUrl(),
                            pg.getHost(),
                            "debezium",
                            pg.getPassword(),
                            ConnectorServiceProto.SourceType.POSTGRES,
                            tableSchema,
                            "test",
                            "orders",
                            true);
            assertEquals("", resp.getError().getErrorMessage());

            query = "REVOKE rds_replication FROM dbz_group";
            SourceTestClient.performQuery(connPg, query);
            query = "GRANT rds_superuser TO dbz_group";
            SourceTestClient.performQuery(connPg, query);
            resp =
                    testClient.validateSource(
                            pg.getJdbcUrl(),
                            pg.getHost(),
                            "debezium",
                            pg.getPassword(),
                            ConnectorServiceProto.SourceType.POSTGRES,
                            tableSchema,
                            "test",
                            "orders",
                            true);
            assertEquals("", resp.getError().getErrorMessage());

        } catch (Exception e) {
            Assert.fail("validate rpc fail: " + e.getMessage());
        } finally {
            // cleanup
            query = testClient.sqlStmts.getProperty("tpch.drop.orders");
            SourceTestClient.performQuery(connPg, query);
            query = "DROP OWNED BY debezium";
            SourceTestClient.performQuery(connPg, query);
            query = "DROP ROLE rds_replication";
            SourceTestClient.performQuery(connPg, query);
            query = "DROP ROLE rds_superuser";
            SourceTestClient.performQuery(connPg, query);
            query = "DROP USER debezium";
            SourceTestClient.performQuery(connPg, query);
            connDbz.close();
            connPg.close();
        }
    }

    // Note: Group is the older version of Role. Normally, only roles should be used in production.
    @Test
    public void testGroupPermissionCheck() throws SQLException {
        // use rds_replication and rds_superuser to simulate the RDS env
        Connection connPg = SourceTestClient.connect(pgDataSource);
        String query = "CREATE ROLE rds_replication";
        SourceTestClient.performQuery(connPg, query);
        query = "CREATE ROLE rds_superuser";
        SourceTestClient.performQuery(connPg, query);
        // user Postgres creates a superuser debezium
        query = "CREATE USER debezium";
        SourceTestClient.performQuery(connPg, query);
        query = "ALTER USER debezium REPLICATION";
        SourceTestClient.performQuery(connPg, query);
        query = "ALTER USER debezium WITH PASSWORD '" + pg.getPassword() + "'";
        SourceTestClient.performQuery(connPg, query);
        query = "CREATE GROUP dbz_group WITH USER debezium";
        SourceTestClient.performQuery(connPg, query);
        query =
                "CREATE TABLE IF NOT EXISTS orders (o_key BIGINT NOT NULL, o_val INT, PRIMARY KEY (o_key))";
        SourceTestClient.performQuery(connPg, query);

        // user debezium connects to Postgres
        DataSource dbzDataSource =
                SourceTestClient.getDataSource(
                        pg.getJdbcUrl(), "debezium", pg.getPassword(), pg.getDriverClassName());
        Connection connDbz = SourceTestClient.connect(dbzDataSource);

        ConnectorServiceProto.TableSchema tableSchema =
                ConnectorServiceProto.TableSchema.newBuilder()
                        .addColumns(
                                PlanCommon.ColumnDesc.newBuilder()
                                        .setName("o_key")
                                        .setColumnType(
                                                Data.DataType.newBuilder()
                                                        .setTypeName(Data.DataType.TypeName.INT64)
                                                        .build())
                                        .build())
                        .addColumns(
                                PlanCommon.ColumnDesc.newBuilder()
                                        .setName("o_val")
                                        .setColumnType(
                                                Data.DataType.newBuilder()
                                                        .setTypeName(Data.DataType.TypeName.INT32)
                                                        .build())
                                        .build())
                        .addPkIndices(0)
                        .build();

        try {
            var resp =
                    testClient.validateSource(
                            pg.getJdbcUrl(),
                            pg.getHost(),
                            "debezium",
                            pg.getPassword(),
                            ConnectorServiceProto.SourceType.POSTGRES,
                            tableSchema,
                            "test",
                            "orders");
            assertEquals(
                    "INVALID_ARGUMENT: Postgres user must have select privilege on table 'public.orders'",
                    resp.getError().getErrorMessage());
            query = "GRANT SELECT ON orders TO dbz_group";
            SourceTestClient.performQuery(connPg, query);

            resp =
                    testClient.validateSource(
                            pg.getJdbcUrl(),
                            pg.getHost(),
                            "debezium",
                            pg.getPassword(),
                            ConnectorServiceProto.SourceType.POSTGRES,
                            tableSchema,
                            "test",
                            "orders");
            assertEquals(
                    "INVALID_ARGUMENT: Postgres user must have create privilege on database 'test'",
                    resp.getError().getErrorMessage());

            query = "GRANT CREATE ON DATABASE test TO dbz_group";
            SourceTestClient.performQuery(connPg, query);

            resp =
                    testClient.validateSource(
                            pg.getJdbcUrl(),
                            pg.getHost(),
                            "debezium",
                            pg.getPassword(),
                            ConnectorServiceProto.SourceType.POSTGRES,
                            tableSchema,
                            "test",
                            "orders");

            assertEquals(
                    "INVALID_ARGUMENT: Postgres user must be the owner of table 'orders' to create/alter publication",
                    resp.getError().getErrorMessage());

            query = "ALTER TABLE orders OWNER TO dbz_group";
            SourceTestClient.performQuery(connPg, query);

            resp =
                    testClient.validateSource(
                            pg.getJdbcUrl(),
                            pg.getHost(),
                            "debezium",
                            pg.getPassword(),
                            ConnectorServiceProto.SourceType.POSTGRES,
                            tableSchema,
                            "test",
                            "orders");

            assertEquals("", resp.getError().getErrorMessage());

            resp =
                    testClient.validateSource(
                            pg.getJdbcUrl(),
                            pg.getHost(),
                            "debezium",
                            pg.getPassword(),
                            ConnectorServiceProto.SourceType.POSTGRES,
                            tableSchema,
                            "test",
                            "orders",
                            true);
            assertEquals(
                    "INVALID_ARGUMENT: Postgres user must be superuser or replication role to start walsender.",
                    resp.getError().getErrorMessage());
            query = "GRANT rds_replication TO dbz_group";
            SourceTestClient.performQuery(connPg, query);

            resp =
                    testClient.validateSource(
                            pg.getJdbcUrl(),
                            pg.getHost(),
                            "debezium",
                            pg.getPassword(),
                            ConnectorServiceProto.SourceType.POSTGRES,
                            tableSchema,
                            "test",
                            "orders",
                            true);
            assertEquals("", resp.getError().getErrorMessage());

            query = "REVOKE rds_replication FROM dbz_group";
            SourceTestClient.performQuery(connPg, query);
            query = "GRANT rds_superuser TO dbz_group";
            SourceTestClient.performQuery(connPg, query);
            resp =
                    testClient.validateSource(
                            pg.getJdbcUrl(),
                            pg.getHost(),
                            "debezium",
                            pg.getPassword(),
                            ConnectorServiceProto.SourceType.POSTGRES,
                            tableSchema,
                            "test",
                            "orders",
                            true);
            assertEquals("", resp.getError().getErrorMessage());

        } catch (Exception e) {
            Assert.fail("validate rpc fail: " + e.getMessage());
        } finally {
            // cleanup
            query = testClient.sqlStmts.getProperty("tpch.drop.orders");
            SourceTestClient.performQuery(connPg, query);
            query = "DROP OWNED BY dbz_group";
            SourceTestClient.performQuery(connPg, query);
            query = "DROP GROUP dbz_group";
            SourceTestClient.performQuery(connPg, query);
            query = "DROP ROLE rds_replication";
            SourceTestClient.performQuery(connPg, query);
            query = "DROP ROLE rds_superuser";
            SourceTestClient.performQuery(connPg, query);
            query = "DROP USER debezium";
            SourceTestClient.performQuery(connPg, query);
            connDbz.close();
        }
    }

    // generates test cases for the risingwave debezium parser
    @Ignore
    @Test
    public void getTestJson() throws SQLException {
        try (Connection connection = SourceTestClient.connect(pgDataSource)) {
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
            query =
                    "INSERT INTO orders (O_KEY, O_BOOL, O_BITS, O_TINY, O_INT, O_REAL, O_DOUBLE, O_DECIMAL, O_CHAR, O_DATE, O_TIME, O_TIMESTAMP, O_JSON, O_TEXT_ARR)"
                            + "VALUES(111, TRUE, b'111', -1, -1111, -11.11, -111.11111, -111.11, 'yes please', '2011-11-11', '11:11:11', '2011-11-11 11:11:11.123456', '{\"k1\": \"v1\", \"k2\": 11}', ARRAY[['meeting', 'lunch'], ['training', 'presentation']])";
            SourceTestClient.performQuery(connection, query);
            if (eventStream.hasNext()) {
                List<ConnectorServiceProto.CdcMessage> messages =
                        eventStream.next().getEventsList();
                for (ConnectorServiceProto.CdcMessage msg : messages) {
                    System.out.printf("%s\n", msg.getPayload());
                }
            }
        }
    }

    @Ignore
    @Test
    public void getTestJsonDateTime() throws SQLException {
        try (Connection connection = SourceTestClient.connect(pgDataSource)) {
            String query =
                    "CREATE TABLE orders ("
                            + "o_key integer, "
                            + "o_time_0 time(0), "
                            + "o_time_6 time(6), "
                            + "o_timez_0 time(0) with time zone, "
                            + "o_timez_6 time(6) with time zone, "
                            + "o_timestamp_0 timestamp(0), "
                            + "o_timestamp_6 timestamp(6), "
                            + "o_timestampz_0 timestamp(0) with time zone, "
                            + "o_timestampz_6 timestamp(6) with time zone, "
                            + "o_interval interval, "
                            + "o_date date, "
                            + "PRIMARY KEY (o_key))";
            SourceTestClient.performQuery(connection, query);
            query =
                    "INSERT INTO orders VALUES (0, '11:11:11', '11:11:11.00001', '11:11:11Z', '11:11:11.00001Z', '2011-11-11 11:11:11', '2011-11-11 11:11:11.123456', '2011-11-11 11:11:11',  '2011-11-11 11:11:11.123456', INTERVAL '1 years 2 months 3 days 4 hours 5 minutes 6.78 seconds', '1999-09-09')";
            SourceTestClient.performQuery(connection, query);
            Iterator<ConnectorServiceProto.GetEventStreamResponse> eventStream =
                    testClient.getEventStreamStart(
                            pg, ConnectorServiceProto.SourceType.POSTGRES, "test", "orders");
            if (eventStream.hasNext()) {
                List<ConnectorServiceProto.CdcMessage> messages =
                        eventStream.next().getEventsList();
                for (ConnectorServiceProto.CdcMessage msg : messages) {
                    System.out.printf("%s\n", msg.getPayload());
                }
            }
        }
    }

    @Ignore
    @Test
    public void getTestJsonNumeric() throws SQLException {
        try (Connection connection = SourceTestClient.connect(pgDataSource)) {
            String query =
                    "CREATE TABLE orders (o_key integer, o_smallint smallint, o_integer integer, o_bigint bigint, o_real real, o_double double precision, o_numeric numeric, o_numeric_6_3 numeric(6,3), o_money money, PRIMARY KEY (o_key))";
            SourceTestClient.performQuery(connection, query);
            query =
                    "INSERT INTO orders VALUES (0, 32767, 2147483647, 9223372036854775807, 9.999, 9.999999, 123456.7890, 123.456, 123.12)";
            SourceTestClient.performQuery(connection, query);
            Iterator<ConnectorServiceProto.GetEventStreamResponse> eventStream =
                    testClient.getEventStreamStart(
                            pg, ConnectorServiceProto.SourceType.POSTGRES, "test", "orders");
            if (eventStream.hasNext()) {
                List<ConnectorServiceProto.CdcMessage> messages =
                        eventStream.next().getEventsList();
                for (ConnectorServiceProto.CdcMessage msg : messages) {
                    System.out.printf("%s", msg.getPayload());
                }
            }
        }
    }

    @Ignore
    @Test
    public void getTestJsonOther() throws SQLException {
        try (Connection connection = SourceTestClient.connect(pgDataSource)) {
            String query = "create type bear as enum ('polar', 'brown', 'panda')";
            SourceTestClient.performQuery(connection, query);
            query =
                    "CREATE TABLE orders ("
                            + "o_key integer,"
                            + "o_boolean boolean,"
                            + "o_bit bit,"
                            + "o_bytea bytea,"
                            + "o_json jsonb,"
                            + "o_xml xml,"
                            + "o_uuid uuid,"
                            + "o_point point,"
                            + "o_enum bear,"
                            + "o_char char,"
                            + "o_varchar varchar,"
                            + "o_character character,"
                            + "o_character_varying character varying,"
                            + "PRIMARY KEY (o_key))";
            SourceTestClient.performQuery(connection, query);
            query =
                    "INSERT INTO orders VALUES (1, 'false', b'1', decode('0123456789ABCDEF', 'hex'), '{\"k1\": \"v1\", \"k2\": 11}', xmlcomment('hahaha'), gen_random_uuid(), point(1, 2), 'polar', 'h', 'ha', 'h', 'hahaha')";
            SourceTestClient.performQuery(connection, query);
            Iterator<ConnectorServiceProto.GetEventStreamResponse> eventStream =
                    testClient.getEventStreamStart(
                            pg, ConnectorServiceProto.SourceType.POSTGRES, "test", "orders");
            if (eventStream.hasNext()) {
                List<ConnectorServiceProto.CdcMessage> messages =
                        eventStream.next().getEventsList();
                for (ConnectorServiceProto.CdcMessage msg : messages) {
                    System.out.printf("%s\n", msg.getPayload());
                }
            }
        }
    }
}
