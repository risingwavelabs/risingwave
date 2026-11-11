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

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.source.SourceTypeE;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresCdcSource extends CdcSource {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresCdcSource.class);

    public PostgresCdcSource(
            long sourceId,
            String startOffset,
            Map<String, String> userProps,
            boolean snapshotDone,
            boolean isCdcSourceJob) {
        super(sourceId, startOffset, userProps, snapshotDone, isCdcSourceJob);
    }

    public static String getJdbcUrl(String host, String port, String database) {
        return String.format("jdbc:postgresql://%s:%s/%s", host, port, database);
    }

    /**
     * Creates the publication named in the properties if it does not exist, unless the properties
     * disable automatic creation.
     */
    public static void createPublication(Map<String, String> properties) throws SQLException {
        boolean pubAutoCreate =
                properties.get(PostgresDbzConnectorConfig.PUB_CREATE).equalsIgnoreCase("true");
        var pubName = properties.get(PostgresDbzConnectorConfig.PUB_NAME);
        if (!pubAutoCreate) {
            LOG.info(
                    "Postgres publication auto creation is disabled, skip creation for publication {}.",
                    pubName);
            return;
        }

        var dbHost = properties.get(DbzConnectorConfig.HOST);
        var dbPort = properties.get(DbzConnectorConfig.PORT);
        var dbName = properties.get(DbzConnectorConfig.DB_NAME);
        var jdbcUrl = getJdbcUrl(dbHost, dbPort, dbName);
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
                var schemaName = properties.get(DbzConnectorConfig.SCHEMA_NAME);
                var tableName = properties.get(DbzConnectorConfig.TABLE_NAME);
                Optional<String> schemaTableName;
                if (StringUtils.isBlank(schemaName) || StringUtils.isBlank(tableName)) {
                    schemaTableName = Optional.empty();
                } else {
                    schemaTableName =
                            Optional.of(
                                    quoteIdentifier(schemaName) + "." + quoteIdentifier(tableName));
                }

                String withClause = "";
                try {
                    // Always create with `WITH ( publish_via_partition_root = true )` to ensure the
                    // CDC of partitioned tables can work correctly.
                    // `publish_via_partition_root` is not supported before v13. Luckily, before
                    // v13, user cannot add a partitioned table into a publication either.
                    if (jdbcConnection.getMetaData().getDatabaseMajorVersion() >= 13) {
                        withClause = " WITH ( publish_via_partition_root = true )";
                    }
                } catch (SQLException e) {
                    throw ValidatorUtils.internalError(e.getMessage());
                }

                // create the publication if it doesn't exist
                String createPublicationSql;
                if (schemaTableName.isPresent()) {
                    createPublicationSql =
                            String.format(
                                    "CREATE PUBLICATION %s FOR TABLE %s%s;",
                                    quoteIdentifier(pubName), schemaTableName.get(), withClause);
                } else {
                    createPublicationSql =
                            String.format(
                                    "CREATE PUBLICATION %s%s;",
                                    quoteIdentifier(pubName), withClause);
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

    @Override
    public SourceTypeE getSourceType() {
        return SourceTypeE.POSTGRES;
    }

    @Override
    public boolean needsOffsetCommitOnCheckpoint() {
        return true;
    }

    @Override
    public boolean needsToWaitForStreamingRunning() {
        return true;
    }

    @Override
    public void validate(TableSchema tableSchema, boolean isBackfillTable) throws Exception {
        var userProps = getUserProps();
        SourceValidator.validateHeartbeatInterval(userProps);
        SourceValidator.validateHeartbeatTableAutoInitialize(userProps, getSourceType());
        SourceValidator.validateQueueMemoryRatio(userProps);
        SourceValidator.ensureRequiredProps(userProps, isCdcSourceJob());
        SourceValidator.ensurePropNotBlank(userProps, DbzConnectorConfig.SCHEMA_NAME);
        SourceValidator.ensurePropNotBlank(userProps, PostgresDbzConnectorConfig.SLOT_NAME);
        SourceValidator.ensurePropNotBlank(userProps, PostgresDbzConnectorConfig.PUB_NAME);
        SourceValidator.ensurePropNotBlank(userProps, PostgresDbzConnectorConfig.PUB_CREATE);
        try (var validator = new PostgresValidator(userProps, tableSchema, isCdcSourceJob())) {
            validator.validateAll();
        }
    }

    @Override
    public void prepareUpstream() throws SQLException {
        createPublication(getUserProps());
    }

    @Override
    protected DbzConnectorConfig createConfig() {
        return new PostgresDbzConnectorConfig(this);
    }

    private static String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }
}
