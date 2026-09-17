/*
 * Copyright 2026 RisingWave Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.risingwave.connector.source.oracle;

import com.risingwave.connector.ConnectorServiceImpl;
import com.risingwave.connector.source.SourceTestClient;
import com.risingwave.connector.source.common.DbzConnectorConfig;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.sql.Connection;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

final class OracleTestFixture implements AutoCloseable {
    static final String CDB = "FREE";
    static final String PDB = "FREEPDB1";
    static final String SOURCE_SCHEMA = "APP";
    static final String SOURCE_TABLE = "CUSTOMERS";
    static final String CONNECTOR_USER = "C##RW_VALIDATOR";
    static final String PASSWORD = "RwTestPass123";

    private static final String DEFAULT_IMAGE =
            "container-registry.oracle.com/database/free:23.9.0.0";
    private static final String ORACLE_DRIVER = "oracle.jdbc.OracleDriver";

    private final GenericContainer<?> container;
    private final Map<String, DataSource> dataSources = new HashMap<>();
    private Server connectorServer;
    private ManagedChannel connectorChannel;
    private SourceTestClient sourceTestClient;

    OracleTestFixture() {
        var image =
                System.getProperty(
                        "oracle.test.image",
                        System.getenv().getOrDefault("ORACLE_TEST_IMAGE", DEFAULT_IMAGE));
        container =
                new GenericContainer<>(image)
                        .withEnv("ORACLE_PWD", PASSWORD)
                        .withExposedPorts(1521)
                        .waitingFor(Wait.forLogMessage(".*DATABASE IS READY TO USE!.*\\n", 1))
                        .withStartupTimeout(Duration.ofMinutes(10));
    }

    void start() throws Exception {
        container.start();
        try (var cdb = connectAsSystem(CDB)) {
            boolean forceLogging;
            boolean supplementalLogging;
            try (var result =
                    SourceTestClient.performQuery(
                            cdb,
                            "SELECT FORCE_LOGGING, SUPPLEMENTAL_LOG_DATA_MIN FROM V$DATABASE")) {
                forceLogging = "YES".equalsIgnoreCase(result.getString("FORCE_LOGGING"));
                supplementalLogging =
                        "YES".equalsIgnoreCase(result.getString("SUPPLEMENTAL_LOG_DATA_MIN"));
            }
            if (!forceLogging) {
                SourceTestClient.performQuery(cdb, "ALTER DATABASE FORCE LOGGING");
            }
            if (!supplementalLogging) {
                SourceTestClient.performQuery(cdb, "ALTER DATABASE ADD SUPPLEMENTAL LOG DATA");
            }
            SourceTestClient.performQuery(
                    cdb,
                    "CREATE USER "
                            + CONNECTOR_USER
                            + " IDENTIFIED BY "
                            + PASSWORD
                            + " CONTAINER=ALL");
            SourceTestClient.performQuery(
                    cdb,
                    "GRANT CREATE SESSION, SET CONTAINER, FLASHBACK ANY TABLE, SELECT ANY TABLE, "
                            + "SELECT ANY TRANSACTION, LOGMINING, LOCK ANY TABLE, CREATE TABLE, "
                            + "CREATE SEQUENCE TO "
                            + CONNECTOR_USER
                            + " CONTAINER=ALL");
            SourceTestClient.performQuery(
                    cdb,
                    "GRANT SELECT_CATALOG_ROLE, EXECUTE_CATALOG_ROLE TO "
                            + CONNECTOR_USER
                            + " CONTAINER=ALL");
        }
        try (var pdb = connectAsSystem(PDB)) {
            SourceTestClient.performQuery(
                    pdb,
                    "CREATE USER "
                            + SOURCE_SCHEMA
                            + " IDENTIFIED BY "
                            + PASSWORD
                            + " QUOTA UNLIMITED ON USERS");
            SourceTestClient.performQuery(
                    pdb, "ALTER USER " + CONNECTOR_USER + " QUOTA UNLIMITED ON USERS");
            SourceTestClient.performQuery(pdb, "CREATE ROLE HEARTBEAT_WRITER");
            SourceTestClient.performQuery(pdb, "GRANT HEARTBEAT_WRITER TO " + CONNECTOR_USER);
        }
        connectorServer =
                ServerBuilder.forPort(0).addService(new ConnectorServiceImpl()).build().start();
        connectorChannel =
                Grpc.newChannelBuilder(
                                "localhost:" + connectorServer.getPort(),
                                InsecureChannelCredentials.create())
                        .build();
        sourceTestClient = new SourceTestClient(connectorChannel);
    }

    SourceTestClient sourceTestClient() {
        return sourceTestClient;
    }

    Connection connectAsSystem(String service) {
        return SourceTestClient.connect(dataSource(service, "SYSTEM", PASSWORD));
    }

    DataSource dataSource(String service, String username, String password) {
        var key = service + "\u0000" + username;
        return dataSources.computeIfAbsent(
                key,
                ignored ->
                        SourceTestClient.getDataSource(
                                jdbcUrl(service), username, password, ORACLE_DRIVER));
    }

    String jdbcUrl(String service) {
        return "jdbc:oracle:thin:@//" + container.getHost() + ":" + port() + "/" + service;
    }

    Map<String, String> sourceProperties() {
        return sourceProperties(SOURCE_TABLE);
    }

    Map<String, String> sourceProperties(String tableName) {
        var properties = new HashMap<String, String>();
        properties.put(DbzConnectorConfig.HOST, container.getHost());
        properties.put(DbzConnectorConfig.PORT, port());
        properties.put(DbzConnectorConfig.USER, CONNECTOR_USER);
        properties.put(DbzConnectorConfig.PASSWORD, PASSWORD);
        properties.put(DbzConnectorConfig.DB_NAME, CDB);
        properties.put(DbzConnectorConfig.ORACLE_PDB_NAME, PDB);
        properties.put(DbzConnectorConfig.ORACLE_SCHEMA_NAME, SOURCE_SCHEMA);
        properties.put(DbzConnectorConfig.TABLE_NAME, tableName);
        return properties;
    }

    private String port() {
        return container.getMappedPort(1521).toString();
    }

    @Override
    public void close() throws Exception {
        if (connectorChannel != null) {
            connectorChannel.shutdownNow();
        }
        if (connectorServer != null) {
            connectorServer.shutdownNow();
        }
        for (var dataSource : dataSources.values()) {
            if (dataSource instanceof AutoCloseable closeable) {
                closeable.close();
            }
        }
        container.stop();
    }
}
