package com.risingwave.connector;

import static org.assertj.core.api.Assertions.*;

import com.risingwave.proto.ConnectorServiceProto.*;
import io.grpc.*;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.MountableFile;

public class MySQLSourceTest {

    private static final Logger logger = LoggerFactory.getLogger(MySQLSourceTest.class.getName());

    private static final MySQLContainer<?> mysql =
            new MySQLContainer<>("mysql:5.7.34")
                    .withDatabaseName("test")
                    .withUsername("root")
                    .withCopyFileToContainer(
                            MountableFile.forClasspathResource("my.cnf"), "/etc/my.cnf")
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
        // start connector server and mysql...
        try {
            connectorServer.start();
            logger.info("connector service started");
            mysql.start();
            logger.info("mysql started");
        } catch (IOException e) {
            fail("IO exception: ", e);
        }
        // check mysql configuration...
        try {
            Connection connection = SourceTestClient.connect(mysql);
            ResultSet resultSet =
                    SourceTestClient.performQuery(
                            connection, testClient.sqlStmts.getProperty("mysql.bin_log"));
            assertThat(resultSet.getString("Value")).isEqualTo("ON").as("MySQL: bin_log ON");
            connection.close();
        } catch (SQLException e) {
            fail("SQL exception: ", e);
        }
    }

    @AfterClass
    public static void cleanup() {
        connectorServer.shutdown();
        mysql.stop();
    }

    @Test
    public void testLines() throws InterruptedException, SQLException {
        Lock lock = new ReentrantLock();
        Condition done  = lock.newCondition();
        Connection connection = SourceTestClient.connect(mysql);
        String query = testClient.sqlStmts.getProperty("tpch.create.orders");
        SourceTestClient.performQuery(connection, query);
        query =
                "LOAD DATA INFILE '/home/orders.tbl' "
                        + "INTO TABLE orders "
                        + "CHARACTER SET UTF8 "
                        + "FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';";
        SourceTestClient.performQuery(connection, query);
        Iterator<GetEventStreamResponse> eventStream =
                testClient.getEventStreamStart(mysql, SourceType.MYSQL, "test", "orders");
        AtomicInteger count = new AtomicInteger();
        Thread t1 =
                new Thread(
                        () -> {
                            while (eventStream.hasNext()) {
                                List<CdcMessage> messages = eventStream.next().getEventsList();
                                for (CdcMessage ignored : messages) {
                                    count.getAndIncrement();
                                }
                                if (count.get() == 10000) {
                                    lock.lock();
                                    try {
                                        done.signal();
                                    } finally {
                                        lock.unlock();
                                    }
                                }
                            }
                        });
        t1.start();
        lock.lock();
        try {
            done.await();
        } finally {
            lock.unlock();
        }
        logger.info("count: {}", count.get());
        assertThat(count.get()).isEqualTo(10000);
        connection.close();
    }

    @Ignore
    @Test
    public void getTestJson() throws InterruptedException, SQLException {
        Connection connection = SourceTestClient.connect(mysql);
        String query =
                "CREATE TABLE IF NOT EXISTS orders ("
                        + "O_KEY BIGINT NOT NULL, "
                        + "O_BOOL BOOLEAN, "
                        + "O_TINY TINYINT, "
                        + "O_INT INT, "
                        + "O_REAL REAL, "
                        + "O_DOUBLE DOUBLE, "
                        + "O_DECIMAL DECIMAL(15, 2), "
                        + "O_CHAR CHAR(15), "
                        + "O_DATE DATE, "
                        + "O_TIME TIME, "
                        + "O_TIMESTAMP TIMESTAMP, "
                        + "O_JSON JSON, "
                        + "PRIMARY KEY (O_KEY))";
        SourceTestClient.performQuery(connection, query);
        Iterator<GetEventStreamResponse> eventStream =
                testClient.getEventStreamStart(mysql, SourceType.MYSQL, "test", "orders");
        AtomicInteger count = new AtomicInteger();
        Thread t1 =
                new Thread(
                        () -> {
                            while (eventStream.hasNext()) {
                                List<CdcMessage> messages = eventStream.next().getEventsList();
                                for (CdcMessage msg : messages) {
                                    count.getAndIncrement();
                                    logger.info("{}", msg.getPayload());
                                }
                            }
                        });
        Thread.sleep(3000);
        t1.start();
        Thread.sleep(3000);
        // Q1: ordinary insert (read)
        query =
                "INSERT INTO orders (O_KEY, O_BOOL, O_TINY, O_INT, O_REAL, O_DOUBLE, O_DECIMAL, O_CHAR, O_DATE, O_TIME, O_TIMESTAMP, O_JSON)"
                        + "VALUES(111, TRUE, -1, -1111, -11.11, -111.11111, -111.11, 'yes please', '2011-11-11', '11:11:11', '2011-11-11 11:11:11.123456', '{\"k1\":\"v1\",\"k2\":11}')";
        SourceTestClient.performQuery(connection, query);
        // Q2: update value of Q1 (value -> new value)
        query =
                "UPDATE orders SET O_BOOL = FALSE, "
                        + "O_TINY = 3, "
                        + "O_INT = 3333, "
                        + "O_REAL = 33.33, "
                        + "O_DOUBLE = 333.33333, "
                        + "O_DECIMAL = 333.33, "
                        + "O_CHAR = 'no thanks', "
                        + "O_DATE = '2012-12-12', "
                        + "O_TIME = '12:12:12', "
                        + "O_TIMESTAMP = '2011-12-12 12:12:12.121212', "
                        + "O_JSON = '{\"k1\":\"v1_updated\",\"k2\":33}' "
                        + "WHERE orders.O_KEY = 111";
        SourceTestClient.performQuery(connection, query);
        // Q3: delete value from Q1
        query = "DELETE FROM orders WHERE orders.O_KEY = 111";
        SourceTestClient.performQuery(connection, query);
        Thread.sleep(5000);
        connection.close();
    }
}
