package com.risingwave.connector;

import static org.assertj.core.api.Assertions.*;
import static org.junit.Assert.assertEquals;

import com.risingwave.proto.ConnectorServiceProto.*;
import io.grpc.*;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import javax.sql.DataSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.MountableFile;

public class MySQLSourceTest {

    static final Logger LOG = LoggerFactory.getLogger(MySQLSourceTest.class.getName());

    private static final MySQLContainer<?> mysql =
            new MySQLContainer<>("mysql:5.7.34")
                    .withDatabaseName("test")
                    .withUsername("root")
                    .withCopyFileToContainer(
                            MountableFile.forClasspathResource("my.cnf"), "/etc/my.cnf");

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

    private static DataSource mysqlDataSource;

    @BeforeClass
    public static void init() {
        // generate orders.tbl test data
        SourceTestClient.genOrdersTable(10000);
        // start connector server and mysql...
        try {
            connectorServer.start();
            LOG.info("connector service started");
            mysql.withCopyFileToContainer(
                    MountableFile.forClasspathResource("orders.tbl"), "/home/orders.tbl");
            mysql.start();
            mysqlDataSource = SourceTestClient.getDataSource(mysql);
            LOG.info("mysql started");
        } catch (IOException e) {
            fail("IO exception: ", e);
        }
        // check mysql configuration...
        try {
            Connection connection = SourceTestClient.connect(mysqlDataSource);
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

    // create a TPC-H orders table in mysql
    // insert 10,000 rows into orders
    // check if the number of changes debezium captures is 10,000
    @Test
    public void testLines() throws InterruptedException, SQLException {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        Connection connection = SourceTestClient.connect(mysqlDataSource);
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
        Callable<Integer> countTask =
                () -> {
                    int count = 0;
                    while (eventStream.hasNext()) {
                        List<CdcMessage> messages = eventStream.next().getEventsList();
                        for (CdcMessage ignored : messages) {
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

    // generates test cases for the risingwave debezium parser
    @Ignore
    @Test
    public void getTestJson() throws InterruptedException, SQLException {
        Connection connection = SourceTestClient.connect(mysqlDataSource);
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
                        + "O_DATETIME DATETIME, "
                        + "O_TIMESTAMP TIMESTAMP, "
                        + "O_JSON JSON, "
                        + "PRIMARY KEY (O_KEY))";
        SourceTestClient.performQuery(connection, query);
        Iterator<GetEventStreamResponse> eventStream =
                testClient.getEventStreamStart(mysql, SourceType.MYSQL, "test", "orders");
        Thread t1 =
                new Thread(
                        () -> {
                            while (eventStream.hasNext()) {
                                List<CdcMessage> messages = eventStream.next().getEventsList();
                                for (CdcMessage msg : messages) {
                                    LOG.info("{}", msg.getPayload());
                                }
                            }
                        });
        Thread.sleep(3000);
        t1.start();
        Thread.sleep(3000);
        // Q1: ordinary insert
        query =
                "INSERT INTO orders (O_KEY, O_BOOL, O_TINY, O_INT, O_REAL, O_DOUBLE, O_DECIMAL, O_CHAR, O_DATE, O_TIME, O_DATETIME, O_TIMESTAMP, O_JSON)"
                        + "VALUES(111, TRUE, -1, -1111, -11.11, -111.11111, -111.11, 'yes please', '1000-01-01', '00:00:00', '1970-01-01 00:00:00', '1970-01-01 00:00:01.000000', '{\"k1\": \"v1\", \"k2\": 11}')";
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
                        + "O_DATE = '9999-12-31', "
                        + "O_TIME = '23:59:59', "
                        + "O_DATETIME = '5138-11-16 09:46:39', "
                        + "O_TIMESTAMP = '2038-01-09 03:14:07', "
                        + "O_JSON = '{\"k1\": \"v1_updated\", \"k2\": 33}' "
                        + "WHERE orders.O_KEY = 111";
        SourceTestClient.performQuery(connection, query);
        // Q3: delete value from Q1
        query = "DELETE FROM orders WHERE orders.O_KEY = 111";
        SourceTestClient.performQuery(connection, query);
        Thread.sleep(5000);
        connection.close();
    }
}
