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

package com.risingwave.connector.source.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.risingwave.connector.cdc.debezium.internal.OpendalSchemaHistory;
import com.risingwave.proto.ConnectorServiceProto.GetEventStreamRequest;
import com.risingwave.proto.ConnectorServiceProto.SourceType;
import io.debezium.connector.mariadb.MariaDbStreamingChangeEventSource;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.ObjectName;
import org.junit.Test;

public class DbzConnectorConfigTest {
    private static DbzConnectorConfig createConfig(
            SourceType sourceType, long sourceId, Map<String, String> userProps) {
        return CdcSourceFactory.createSource(
                        GetEventStreamRequest.newBuilder()
                                .setSourceId(sourceId)
                                .setSourceType(sourceType)
                                .putAllProperties(userProps)
                                .build())
                .getConfig();
    }

    private static DbzConnectorConfig postgresConfig(long sourceId) {
        return createConfig(
                SourceType.POSTGRES,
                sourceId,
                Map.of("cdc.source.wait.streaming.start.timeout", "3600"));
    }

    @Test
    public void postgresJdbcUrlPreservesLiteralDatabaseName() {
        for (var database :
                new String[] {
                    "sales",
                    "sales+east",
                    "sales%2Beast",
                    "sales%",
                    "sales?sslmode=disable",
                    "sales?user=other&password=other",
                    "sales east",
                    "sales/east",
                    "sales#east",
                    "sales&east=west",
                    "sales\"east",
                    "销售"
                }) {
            var config =
                    (PostgresDbzConnectorConfig)
                            createConfig(
                                    SourceType.POSTGRES,
                                    46,
                                    Map.of(
                                            "hostname", "example.invalid",
                                            "port", "5432",
                                            "database.name", database));
            var expected = new Properties();
            expected.setProperty("PGHOST", "example.invalid");
            expected.setProperty("PGPORT", "5432");
            expected.setProperty("PGDBNAME", database);

            // Exercise the URL shared by validation and publication creation without connecting.
            assertEquals(
                    database,
                    expected,
                    org.postgresql.Driver.parseURL(config.getJdbcUrl(), new Properties()));
            assertEquals(database, config.getDbName());
            assertEquals(
                    database, config.getResolvedDebeziumProps().getProperty("database.dbname"));
        }
    }

    @Test
    public void usesConstructorSourceIdForTopicPrefix() {
        var userProps = new HashMap<String, String>();
        userProps.put("mongodb.url", "mongodb://localhost:27017");
        userProps.put("collection.name", "test.users");
        userProps.put("source.id", "user-supplied-value");

        var config = createConfig(SourceType.MONGODB, 42, userProps);

        assertEquals("RW_CDC_42", config.getResolvedDebeziumProps().getProperty("topic.prefix"));
    }

    @Test
    public void configuresDedicatedMariaDbConnectorAndSslMode() {
        var userProps = new HashMap<String, String>();
        userProps.put("hostname", "localhost");
        userProps.put("port", "3306");
        userProps.put("username", "root");
        userProps.put("password", "secret");
        userProps.put("database.name", "test");
        userProps.put("table.name", "orders");
        userProps.put("server.id", "4102");
        userProps.put("ssl.mode", "required");

        var config = createConfig(SourceType.MARIADB, 43, userProps);

        assertEquals(
                "io.debezium.connector.mariadb.MariaDbConnector",
                config.getResolvedDebeziumProps().getProperty("connector.class"));
        assertEquals("trust", config.getResolvedDebeziumProps().getProperty("database.ssl.mode"));
        assertEquals(
                OpendalSchemaHistory.class.getName(),
                config.getResolvedDebeziumProps().getProperty("schema.history.internal"));
    }

    @Test
    public void resolvesMariaDbSslModeAlias() {
        var userProps = new HashMap<String, String>();
        for (var mode : new String[] {"disabled", "required", "verify-ca", "verify-full"}) {
            userProps.put("debezium.database.sslmode", mode);
            var config = createConfig(SourceType.MARIADB, 43, userProps);
            assertEquals(
                    MariadbDbzConnectorConfig.normalizeSslMode(mode),
                    config.getResolvedDebeziumProps().getProperty("database.ssl.mode"));
        }
    }

    @Test
    public void prefersCanonicalMariaDbSslModeAndDefaultsToDisabled() {
        var userProps = new HashMap<String, String>();
        var config = createConfig(SourceType.MARIADB, 43, userProps);
        assertEquals("disable", config.getResolvedDebeziumProps().getProperty("database.ssl.mode"));

        userProps.put("debezium.database.sslmode", "disabled");
        userProps.put("ssl.mode", "verify-full");
        config = createConfig(SourceType.MARIADB, 43, userProps);
        assertEquals(
                "verify-full", config.getResolvedDebeziumProps().getProperty("database.ssl.mode"));
    }

    @Test
    public void rejectsUnsupportedMariaDbPreferredSslModeAlias() {
        var userProps = new HashMap<String, String>();
        userProps.put("debezium.database.sslmode", "preferred");
        assertThrows(
                IllegalArgumentException.class,
                () -> createConfig(SourceType.MARIADB, 43, userProps));
    }

    @Test
    public void rejectsUnsupportedMariaDbPreferredSslMode() {
        var exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> MariadbDbzConnectorConfig.normalizeSslMode("preferred"));

        assertEquals(
                "MariaDB CDC does not support ssl.mode 'preferred'; use 'required', 'verify-ca', or"
                        + " 'verify-full'",
                exception.getMessage());
    }

    @Test
    public void forwardsMariaDbRootCertificateAndLegacyAlias() {
        var userProps = new HashMap<String, String>();
        userProps.put("ssl.mode", "verify-full");
        userProps.put("debezium.database.sslrootcert", "/etc/mariadb/legacy-ca.pem");
        var config = createConfig(SourceType.MARIADB, 44, userProps);
        assertEquals(
                "/etc/mariadb/legacy-ca.pem",
                config.getResolvedDebeziumProps().getProperty("driver.serverSslCert"));

        userProps.put("ssl.root.cert", "/etc/mariadb/ca.pem");
        config = createConfig(SourceType.MARIADB, 44, userProps);
        assertEquals(
                "/etc/mariadb/ca.pem",
                config.getResolvedDebeziumProps().getProperty("driver.serverSslCert"));
        assertEquals(
                "/etc/mariadb/ca.pem",
                config.getResolvedDebeziumProps()
                        .getProperty(MariaDbStreamingChangeEventSource.SSL_ROOT_CERT_CONFIG));
    }

    @Test
    public void rejectsQueueMemoryRatioOutOfRange() {
        var exception =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                createConfig(
                                        SourceType.POSTGRES,
                                        45,
                                        Map.of("debezium.queue.memory.ratio", "0.9")));
        assertTrue(exception.getMessage().contains("must be between"));
    }

    @Test(timeout = 5000)
    public void receiverClosureCancelsLongReadinessWait() throws Exception {
        var polls = new AtomicInteger();
        assertFalse(postgresConfig(1).waitForStreamingRunning(() -> polls.incrementAndGet() < 2));
        assertTrue(polls.get() >= 2);
    }

    @Test(timeout = 5000)
    public void interruptionCancelsReadinessWait() {
        assertThrows(
                InterruptedException.class,
                () ->
                        postgresConfig(2)
                                .waitForStreamingRunning(
                                        () -> {
                                            Thread.currentThread().interrupt();
                                            return true;
                                        }));
    }

    @Test(timeout = 5000)
    public void cancellationTakesPrecedenceOverReadiness() throws Exception {
        var server = ManagementFactory.getPlatformMBeanServer();
        var name =
                new ObjectName(
                        "debezium.postgres:type=connector-metrics,context=streaming,server=RW_CDC_3");
        server.registerMBean(new StreamingMetrics(), name);
        try {
            assertFalse(postgresConfig(3).waitForStreamingRunning(() -> false));
            assertTrue(postgresConfig(3).waitForStreamingRunning(() -> true));
        } finally {
            server.unregisterMBean(name);
        }
    }

    @Test(timeout = 5000)
    public void readinessStillTimesOut() throws Exception {
        var config =
                createConfig(
                        SourceType.POSTGRES,
                        4,
                        Map.of("cdc.source.wait.streaming.start.timeout", "0"));
        assertFalse(config.waitForStreamingRunning(() -> true));
    }

    @Test
    public void offsetCommitOnCheckpointFollowsTheReaderProperty() {
        var key = "cdc.source.needs.offset.commit.on.checkpoint";
        assertTrue(
                createConfig(SourceType.POSTGRES, 5, Map.of(key, "true"))
                        .needsOffsetCommitOnCheckpoint());
        assertFalse(
                createConfig(SourceType.POSTGRES, 5, Map.of(key, "false"))
                        .needsOffsetCommitOnCheckpoint());
        assertFalse(createConfig(SourceType.POSTGRES, 5, Map.of()).needsOffsetCommitOnCheckpoint());
    }

    public interface StreamingMetricsMBean {
        boolean isConnected();
    }

    public static class StreamingMetrics implements StreamingMetricsMBean {
        @Override
        public boolean isConnected() {
            return true;
        }
    }
}
