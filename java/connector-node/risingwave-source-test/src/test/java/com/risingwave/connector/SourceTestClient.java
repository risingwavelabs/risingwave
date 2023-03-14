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
        blockingStub = ConnectorServiceGrpc.newBlockingStub(channel);
        try (InputStream input =
                getClass().getClassLoader().getResourceAsStream("stored_queries.properties")) {
            sqlStmts.load(input);
        } catch (IOException e) {
            fail("failed to load sql statements", e);
        }
    }

    protected static Connection connect(DataSource dataSource) {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
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

    // generates an orders.tbl in class path using random data
    // if file does not contain 10000 lines
    static void genOrdersTable(int numRows) {
        String[] orderStatusArr = {"O", "F"};
        String[] orderPriorityArr = {"1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"};
        String path =
                SourceTestClient.class.getProtectionDomain().getCodeSource().getLocation().getFile()
                        + "orders.tbl";
        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            int lines = 0;
            while (reader.readLine() != null) {
                lines++;
            }
            if (lines == 10000) {
                LOG.info("orders.tbl contains 10000 lines, skipping data generation");
                return;
            }
        } catch (Exception e) {
            fail("Runtime Exception: {}", e);
        }
        Random rand = new Random();
        try (PrintWriter writer = new PrintWriter(path, "UTF-8")) {
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
        } catch (Exception e) {
            fail("Runtime Exception: {}", e);
        }
        LOG.info("10000 lines written to orders.tbl");
    }
}
