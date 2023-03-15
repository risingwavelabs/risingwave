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

import com.google.common.collect.Lists;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkBase;
import com.risingwave.connector.api.sink.SinkFactory;
import io.grpc.Status;
import java.sql.*;
import java.util.HashSet;
import java.util.List;
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
    public TableSchema validate(TableSchema tableSchema, Map<String, String> tableProperties) {
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

        try {
            Connection conn = DriverManager.getConnection(jdbcUrl);

            ResultSet columnResultSet = conn.getMetaData().getColumns(null, null, tableName, null);
            while (columnResultSet.next()) {
                jdbcColumns.add(columnResultSet.getString("COLUMN_NAME"));
            }

            ResultSet pkResultSet = conn.getMetaData().getPrimaryKeys(null, null, tableName);
            while (pkResultSet.next()) {
                jdbcPk.add(pkResultSet.getString("COLUMN_NAME"));
            }

            conn.close();
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

        List<String> sinkPk = tableSchema.getPrimaryKeys();
        if (sinkPk.isEmpty()) {
            // The user doesn't specify sink pk. We use the primary key of the JDBC table as sink
            // pk.
            LOG.info(
                    "user-defined sink pk is empty, use jdbc table {}'s pk: {}",
                    tableName,
                    jdbcPk.toString());
            return new TableSchema(tableSchema, Lists.newArrayList(jdbcPk));
        } else {
            return tableSchema;
        }
    }
}
