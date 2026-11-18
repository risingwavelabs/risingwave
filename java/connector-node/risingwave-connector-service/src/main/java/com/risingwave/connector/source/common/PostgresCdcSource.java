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

import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.proto.ConnectorServiceProto.GetEventStreamRequest;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresCdcSource extends CdcSource {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresCdcSource.class);

    PostgresCdcSource(GetEventStreamRequest request) {
        super(request);
    }

    @Override
    public SourceTypeE getSourceType() {
        return SourceTypeE.POSTGRES;
    }

    @Override
    protected DbzConnectorConfig createConfig(GetEventStreamRequest request) {
        return new PostgresDbzConnectorConfig(request);
    }

    /**
     * Creates the publication named in the WITH options if it does not exist, unless the options
     * disable automatic creation. Validation of a shared source and the reader, before the Debezium
     * engine starts, both call it.
     */
    public static void createPublication(PostgresDbzConnectorConfig config) throws SQLException {
        var pubName = config.getPubName();
        if (!config.isPubAutoCreate()) {
            LOG.info(
                    "Postgres publication auto creation is disabled, skip creation for publication {}.",
                    pubName);
            return;
        }

        try (var jdbcConnection =
                DriverManager.getConnection(
                        config.getJdbcUrl(), config.getUser(), config.getPassword())) {
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
                var schemaName = config.getSchemaName();
                var tableName = config.getTableName();
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

    private static String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }
}
