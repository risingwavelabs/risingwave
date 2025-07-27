// Copyright 2025 RisingWave Labs
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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkFactory;
import com.risingwave.connector.api.sink.SinkWriter;
import com.risingwave.proto.Catalog.SinkType;
import io.grpc.Status;
import java.sql.*;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCSinkFactory implements SinkFactory {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCSinkFactory.class);

    public static final String JDBC_URL_PROP = "jdbc.url";
    public static final String TABLE_NAME_PROP = "table.name";

    @Override
    public SinkWriter createWriter(TableSchema tableSchema, Map<String, String> tableProperties) {
        ObjectMapper mapper = new ObjectMapper();
        JDBCSinkConfig config = mapper.convertValue(tableProperties, JDBCSinkConfig.class);
        if ((config.getJdbcUrl().startsWith("jdbc:snowflake")
                || config.getJdbcUrl().startsWith("jdbc:redshift"))) {
            return new BatchAppendOnlyJDBCSink(config, tableSchema);
        }
        return new JDBCSink(config, tableSchema);
    }

    @Override
    public void validate(
            TableSchema tableSchema, Map<String, String> tableProperties, SinkType sinkType) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES, true);
        JDBCSinkConfig config = mapper.convertValue(tableProperties, JDBCSinkConfig.class);

        String jdbcUrl = config.getJdbcUrl();
        String tableName = config.getTableName();
        String schemaName = config.getSchemaName();
        Set<String> jdbcColumns = new HashSet<>();
        Set<String> jdbcPks = new HashSet<>();
        Set<String> jdbcTableNames = new HashSet<>();

        try (Connection conn =
                        DriverManager.getConnection(
                                jdbcUrl, config.getUser(), config.getPassword());
                ResultSet tableNamesResultSet =
                        conn.getMetaData().getTables(null, schemaName, "%", null);
                ResultSet columnResultSet =
                        conn.getMetaData().getColumns(null, schemaName, tableName, null);
                ResultSet pkResultSet =
                        conn.getMetaData().getPrimaryKeys(null, schemaName, tableName); ) {
            while (tableNamesResultSet.next()) {
                jdbcTableNames.add(tableNamesResultSet.getString("TABLE_NAME"));
            }
            while (columnResultSet.next()) {
                jdbcColumns.add(columnResultSet.getString("COLUMN_NAME"));
            }
            while (pkResultSet.next()) {
                jdbcPks.add(pkResultSet.getString("COLUMN_NAME"));
            }
        } catch (SQLException e) {
            LOG.error("failed to connect to target database. jdbcUrl: {}", jdbcUrl, e);
            throw Status.INVALID_ARGUMENT
                    .withDescription(
                            "failed to connect to target database: "
                                    + e.getSQLState()
                                    + ": "
                                    + e.getMessage())
                    .asRuntimeException();
        }

        if (!jdbcTableNames.contains(tableName)) {
            throw Status.INVALID_ARGUMENT
                    .withDescription("table not found: " + tableName)
                    .asRuntimeException();
        }

        // Check that all columns in tableSchema exist in the JDBC table.
        for (String sinkColumn : tableSchema.getColumnNames()) {
            if (!jdbcColumns.contains(sinkColumn)) {
                LOG.error("column not found: {}", sinkColumn);
                throw Status.FAILED_PRECONDITION
                        .withDescription(
                                "table schema does not match, column not found: " + sinkColumn)
                        .asRuntimeException();
            }
        }

        if (sinkType == SinkType.SINK_TYPE_UPSERT) {
            // For upsert JDBC sink, the primary key defined on the table must match the one in
            // config and cannot be empty
            var pkInWith = new HashSet<>(tableSchema.getPrimaryKeys());
            if (jdbcPks.isEmpty() || !jdbcPks.equals(pkInWith)) {
                throw Status.INVALID_ARGUMENT
                        .withDescription(
                                "JDBC table has no primary key or the primary key doesn't match the 'primary_key' option in the WITH clause")
                        .asRuntimeException();
            }
            if (tableSchema.getPrimaryKeys().isEmpty()) {
                throw Status.INVALID_ARGUMENT
                        .withDescription("Must specify downstream primary key for upsert JDBC sink")
                        .asRuntimeException();
            }
        }
    }
}
