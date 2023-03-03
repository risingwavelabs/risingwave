package com.risingwave.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.risingwave.proto.ConnectorServiceProto;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

public class PostgresSourceTest {
    private static final Logger logger =
            LoggerFactory.getLogger(PostgresSourceTest.class.getName());

    private static final PostgreSQLContainer<?> pg =
            new PostgreSQLContainer<>("postgres:12.3-alpine")
                    .withDatabaseName("test")
                    .withUsername("postgres")
                    .withCommand("postgres -c wal_level=logical -c max_wal_senders=10")
                    .withCopyFileToContainer(
                            MountableFile.forClasspathResource("orders.tbl"), "/home/orders.tbl");

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

    @BeforeClass
    public static void init() {
        // start connector server and postgres...
        try {
            connectorServer.start();
            logger.info("connector service started");
            pg.start();
            pg.withUsername("postgres")
                    .execInContainer(
                            "sh",
                            "-c",
                            "echo 'host replication postgres 172.17.0.1/32 trust' >> /var/lib/postgresql/data/pg_hba.conf");
            logger.info("postgres started");
        } catch (IOException e) {
            fail("IO exception: ", e);
        } catch (InterruptedException e) {
            fail("Interrupted exception", e);
        }
        // check pg configuration...
        try {
            Connection connection = SourceTestClient.connect(pg);
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

    @Test
    public void testLines() throws InterruptedException, SQLException {
        Connection connection = SourceTestClient.connect(pg);
        String query = testClient.sqlStmts.getProperty("tpch.create.orders");
        SourceTestClient.performQuery(connection, query);
        Iterator<ConnectorServiceProto.GetEventStreamResponse> eventStream =
                testClient.getEventStreamStart(
                        pg, ConnectorServiceProto.SourceType.POSTGRES, "test", "orders");
        AtomicInteger count = new AtomicInteger();
        Thread t1 =
                new Thread(
                        () -> {
                            while (eventStream.hasNext()) {
                                List<ConnectorServiceProto.CdcMessage> messages =
                                        eventStream.next().getEventsList();
                                for (ConnectorServiceProto.CdcMessage ignored : messages) {
                                    count.getAndIncrement();
                                }
                            }
                        });
        t1.start();
        Thread.sleep(10000);
        query = "COPY orders FROM '/home/orders.tbl' WITH DELIMITER '|'";
        SourceTestClient.performQuery(connection, query);
        Thread.sleep(10000);
        logger.info("count: {}", count.get());
        assertThat(count.get()).isEqualTo(10000);
        connection.close();
    }

    @Ignore
    @Test
    public void getTestJson() throws InterruptedException, SQLException {
        Connection connection = SourceTestClient.connect(pg);
        //        String query =
        //                "CREATE TYPE COMPLEX AS (r DOUBLE PRECISION, i DOUBLE PRECISION)";
        //        SourceTestClient.performQuery(connection, query);
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
        AtomicInteger count = new AtomicInteger();
        Thread t1 =
                new Thread(
                        () -> {
                            while (eventStream.hasNext()) {
                                List<ConnectorServiceProto.CdcMessage> messages =
                                        eventStream.next().getEventsList();
                                for (ConnectorServiceProto.CdcMessage msg : messages) {
                                    count.getAndIncrement();
                                    logger.info("{}", msg.getPayload());
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
        logger.info("count: {}", count.get());
        connection.close();
    }
}
