package com.risingwave.connector;

import static org.assertj.core.api.Assertions.fail;

import com.risingwave.proto.ConnectorServiceGrpc;
import com.risingwave.proto.ConnectorServiceProto;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import java.io.*;
import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;

public class SourceTestClient {
    static final Logger LOG = LoggerFactory.getLogger(SourceTestClient.class.getName());
    private final ConnectorServiceGrpc.ConnectorServiceBlockingStub blockingStub;

    public Properties sqlStmts = new Properties();

    public SourceTestClient(Channel channel) {
        blockingStub =
                ConnectorServiceGrpc.newBlockingStub(channel).withMaxInboundMessageSize(16777216);
        try (InputStream input =
                getClass().getClassLoader().getResourceAsStream("stored_queries.properties")) {
            sqlStmts.load(input);
        } catch (IOException e) {
            fail("failed to load sql statements", e);
        }
    }

    protected static Connection connect(JdbcDatabaseContainer<?> container) {
        Connection connection = null;
        try {
            connection = getDataSource(container).getConnection();
        } catch (SQLException e) {
            fail("SQL Exception: {}", e);
        }
        return connection;
    }

    protected static ResultSet performQuery(Connection connection, String sql) {
        ResultSet resultSet = null;
        try {
            Statement statement = connection.createStatement();
            if (statement.execute(sql)) {
                resultSet = statement.getResultSet();
                resultSet.next();
            } else {
                LOG.info("updated: " + statement.getUpdateCount());
            }
        } catch (SQLException e) {
            LOG.warn("SQL Exception: {}", e.getMessage());
        }
        return resultSet;
    }

    protected static DataSource getDataSource(JdbcDatabaseContainer<?> container) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(container.getJdbcUrl());
        hikariConfig.setUsername(container.getUsername());
        hikariConfig.setPassword(container.getPassword());
        hikariConfig.setDriverClassName(container.getDriverClassName());
        return new HikariDataSource(hikariConfig);
    }

    protected Iterator<ConnectorServiceProto.GetEventStreamResponse> getEventStreamStart(
            JdbcDatabaseContainer<?> container,
            ConnectorServiceProto.SourceType sourceType,
            String databaseName,
            String tableName) {
        String port = String.valueOf(URI.create(container.getJdbcUrl().substring(5)).getPort());
        ConnectorServiceProto.GetEventStreamRequest req =
                ConnectorServiceProto.GetEventStreamRequest.newBuilder()
                        .setStart(
                                ConnectorServiceProto.GetEventStreamRequest.StartSource.newBuilder()
                                        .setSourceId(0)
                                        .setSourceType(sourceType)
                                        .setStartOffset("")
                                        .putProperties("hostname", container.getHost())
                                        .putProperties("port", port)
                                        .putProperties("username", container.getUsername())
                                        .putProperties("password", container.getPassword())
                                        .putProperties("database.name", databaseName)
                                        .putProperties("table.name", tableName)
                                        .putProperties("schema.name", "public") // pg only
                                        .putProperties("slot.name", "orders") // pg only
                                        .putProperties("server.id", "1")) // mysql only
                        .build();
        Iterator<ConnectorServiceProto.GetEventStreamResponse> responses = null;
        try {
            responses = blockingStub.getEventStream(req);
        } catch (StatusRuntimeException e) {
            fail("RPC failed: {}", e.getStatus());
        }
        return responses;
    }

    private static String[] orderStatusArr = {"O", "F"};
    private static String[] orderPriorityArr = {
        "1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"
    };

    // generates an orders table in class path if not exists
    // data completely random
    static void genOrdersTable(int numRows) {
        String path =
                PostgresSourceTest.class
                        .getProtectionDomain()
                        .getCodeSource()
                        .getLocation()
                        .getFile();
        Random rand = new Random();
        PrintWriter writer = null;
        try {
            writer = new PrintWriter(path + "orders.tbl", "UTF-8");
        } catch (Exception e) {
            fail("Runtime Exception: {}", e);
        }
        assert writer != null;
        for (int i = 1; i <= numRows; i++) {
            String custKey = String.valueOf(Math.abs(rand.nextLong()));
            String orderStatus = orderStatusArr[rand.nextInt(orderStatusArr.length)];
            String totalPrice = rand.nextInt(1000000) + "." + rand.nextInt(9) + rand.nextInt(9);
            String orderDate =
                    (rand.nextInt(60) + 1970)
                            + "-"
                            + String.format("%02d", rand.nextInt(12) + 1)
                            + "-"
                            + String.format("%02d", rand.nextInt(28) + 1);
            String orderPriority = orderPriorityArr[rand.nextInt(orderPriorityArr.length)];
            String clerk = "Clerk#" + String.format("%09d", rand.nextInt(1024));
            String shipPriority = "0";
            String comment = UUID.randomUUID() + " " + UUID.randomUUID();
            writer.printf(
                    "%s|%s|%s|%s|%s|%s|%s|%s|%s\n",
                    i,
                    custKey,
                    orderStatus,
                    totalPrice,
                    orderDate,
                    orderPriority,
                    clerk,
                    shipPriority,
                    comment);
        }
        writer.close();
    }
}
