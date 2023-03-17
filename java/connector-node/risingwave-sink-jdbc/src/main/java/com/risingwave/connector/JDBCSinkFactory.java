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

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkBase;
import com.risingwave.connector.api.sink.SinkFactory;
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
    public SinkBase create(TableSchema tableSchema, Map<String, String> tableProperties) {
        String tableName = tableProperties.get(TABLE_NAME_PROP);
        String jdbcUrl = tableProperties.get(JDBC_URL_PROP);
        return new JDBCSink(tableName, jdbcUrl, tableSchema);
    }

    @Override
    public void validate(
            TableSchema tableSchema, Map<String, String> tableProperties, SinkType sinkType) {
        if (!tableProperties.containsKey(JDBC_URL_PROP)
                || !tableProperties.containsKey(TABLE_NAME_PROP)) {
            throw Status.INVALID_ARGUMENT
                    .withDescription(
                            String.format(
                                    "%s or %s is not specified", JDBC_URL_PROP, TABLE_NAME_PROP))
                    .asRuntimeException();
        }

        String jdbcUrl = tableProperties.get(JDBC_URL_PROP);
        String tableName = tableProperties.get(TABLE_NAME_PROP);
        Set<String> jdbcColumns = new HashSet<>();
        Set<String> jdbcPk = new HashSet<>();

        try (Connection conn = DriverManager.getConnection(jdbcUrl);
                ResultSet columnResultSet =
                        conn.getMetaData().getColumns(null, null, tableName, null);
                ResultSet pkResultSet =
                        conn.getMetaData().getPrimaryKeys(null, null, tableName); ) {
            while (columnResultSet.next()) {
                jdbcColumns.add(columnResultSet.getString("COLUMN_NAME"));
            }
            while (pkResultSet.next()) {
                jdbcPk.add(pkResultSet.getString("COLUMN_NAME"));
            }
        } catch (SQLException e) {
            throw Status.INTERNAL.withCause(e).asRuntimeException();
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

        if (sinkType == SinkType.UPSERT) {
            // For JDBC sink, we enforce the primary key as that of the JDBC table's. The JDBC table
            // must have primary key.
            if (jdbcPk.isEmpty()) {
                throw Status.INVALID_ARGUMENT
                        .withDescription(
                                "JDBC table has no primary key, consider making the sink append-only or defining primary key on the JDBC table")
                        .asRuntimeException();
            }
            // The user is not allowed to define the primary key for upsert JDBC sink.
            if (!tableSchema.getPrimaryKeys().isEmpty()) {
                throw Status.INVALID_ARGUMENT
                        .withDescription(
                                "should not define primary key on upsert JDBC sink, find downstream primary key: "
                                        + jdbcPk.toString())
                        .asRuntimeException();
            }
        }
    }
}
