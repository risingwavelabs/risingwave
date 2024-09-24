// Copyright 2024 RisingWave Labs
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

package com.risingwave.connector.source.common;

import com.risingwave.connector.api.source.SourceTypeE;
import java.lang.management.ManagementFactory;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbzSourceUtils {
    static final Logger LOG = LoggerFactory.getLogger(DbzSourceUtils.class);

    /**
     * This method is used to create a publication for the cdc source job or cdc table if it doesn't
     * exist.
     */
    public static void createPostgresPublicationIfNeeded(
            Map<String, String> properties, long sourceId) throws SQLException {
        boolean pubAutoCreate =
                properties.get(DbzConnectorConfig.PG_PUB_CREATE).equalsIgnoreCase("true");
        if (!pubAutoCreate) {
            LOG.info(
                    "Postgres publication auto creation is disabled, skip creation for source {}.",
                    sourceId);
            return;
        }

        var pubName = properties.get(DbzConnectorConfig.PG_PUB_NAME);
        var dbHost = properties.get(DbzConnectorConfig.HOST);
        var dbPort = properties.get(DbzConnectorConfig.PORT);
        var dbName = properties.get(DbzConnectorConfig.DB_NAME);
        var jdbcUrl = ValidatorUtils.getJdbcUrl(SourceTypeE.POSTGRES, dbHost, dbPort, dbName);
        var user = properties.get(DbzConnectorConfig.USER);
        var password = properties.get(DbzConnectorConfig.PASSWORD);
        try (var jdbcConnection = DriverManager.getConnection(jdbcUrl, user, password)) {
            boolean isPubExist = false;
            try (var stmt =
                    jdbcConnection.prepareStatement(
                            ValidatorUtils.getSql("postgres.publication_exist"))) {
                stmt.setString(1, pubName);
                var res = stmt.executeQuery();
                if (res.next()) {
                    isPubExist = res.getBoolean(1);
                }
            }

            if (!isPubExist) {
                var schemaName = properties.get(DbzConnectorConfig.PG_SCHEMA_NAME);
                var tableName = properties.get(DbzConnectorConfig.TABLE_NAME);
                Optional<String> schemaTableName;
                if (StringUtils.isBlank(schemaName) || StringUtils.isBlank(tableName)) {
                    schemaTableName = Optional.empty();
                } else {
                    schemaTableName =
                            Optional.of(quotePostgres(schemaName) + "." + quotePostgres(tableName));
                }

                // create the publication if it doesn't exist
                String createPublicationSql;
                if (schemaTableName.isPresent()) {
                    createPublicationSql =
                            String.format(
                                    "CREATE PUBLICATION %s FOR TABLE %s WITH ( publish_via_partition_root = true );",
                                    quotePostgres(pubName), schemaTableName.get());
                } else {
                    createPublicationSql =
                            String.format(
                                    "CREATE PUBLICATION %s WITH ( publish_via_partition_root = true );",
                                    quotePostgres(pubName));
                }
                try (var stmt = jdbcConnection.createStatement()) {
                    LOG.info(
                            "Publication '{}' doesn't exist, created publication with statement: {}",
                            pubName,
                            createPublicationSql);
                    stmt.execute(createPublicationSql);
                }
            }
        }
    }

    private static String quotePostgres(String identifier) {
        return "\"" + identifier + "\"";
    }

    public static boolean waitForStreamingRunning(
            SourceTypeE sourceType, String dbServerName, int waitStreamingStartTimeout) {
        // Wait for streaming source of source that supported backfill
        LOG.info("Waiting for streaming source of {} to start", dbServerName);
        if (sourceType == SourceTypeE.MYSQL) {
            return waitForStreamingRunningInner("mysql", dbServerName, waitStreamingStartTimeout);
        } else if (sourceType == SourceTypeE.POSTGRES) {
            return waitForStreamingRunningInner(
                    "postgres", dbServerName, waitStreamingStartTimeout);
        } else if (sourceType == SourceTypeE.SQL_SERVER) {
            return waitForStreamingRunningInner(
                    "sql_server", dbServerName, waitStreamingStartTimeout);
        } else {
            LOG.info("Unsupported backfill source, just return true for {}", dbServerName);
            return true;
        }
    }

    private static boolean waitForStreamingRunningInner(
            String connector, String dbServerName, int waitStreamingStartTimeout) {
        int pollCount = 0;
        while (!isStreamingRunning(connector, dbServerName, "streaming")) {
            if (pollCount > waitStreamingStartTimeout) {
                LOG.error(
                        "Debezium streaming source of {} failed to start in timeout {}",
                        dbServerName,
                        waitStreamingStartTimeout);
                return false;
            }
            try {
                TimeUnit.SECONDS.sleep(1); // poll interval
                pollCount++;
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while waiting for streaming source to start", e);
            }
        }

        LOG.info("Debezium streaming source of {} started", dbServerName);
        return true;
    }

    // Copy from debezium test suite: io.debezium.embedded.AbstractConnectorTest
    // Notes: although this method is recommended by the community
    // (https://debezium.zulipchat.com/#narrow/stream/302529-community-general/topic/.E2.9C.94.20Embedded.20engine.20has.20started.20StreamingChangeEventSource/near/405121659),
    // but it is not solid enough. As the jmx bean metric is marked as true before starting the
    // binlog client, which may fail to connect the upstream database.
    private static boolean isStreamingRunning(String connector, String server, String contextName) {
        final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        try {
            return (boolean)
                    mbeanServer.getAttribute(
                            getStreamingMetricsObjectName(connector, server, contextName),
                            "Connected");
        } catch (JMException _ex) {
            // ignore the exception, as it is expected when the streaming source
            // (aka. binlog client) is not ready
        }
        return false;
    }

    private static ObjectName getStreamingMetricsObjectName(
            String connector, String server, String context) throws MalformedObjectNameException {
        if (Objects.equals(connector, "sql_server")) {
            // TODO: fulfill the task id here, by WKX
            return new ObjectName(
                    "debezium."
                            + connector
                            + ":type=connector-metrics,task=0,context="
                            + context
                            + ",server="
                            + server);
        } else {
            return new ObjectName(
                    "debezium."
                            + connector
                            + ":type=connector-metrics,context="
                            + context
                            + ",server="
                            + server);
        }
    }
}
