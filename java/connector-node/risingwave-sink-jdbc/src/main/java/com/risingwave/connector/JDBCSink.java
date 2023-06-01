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
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.proto.Data;
import io.grpc.Status;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

enum DatabaseType {
    MYSQL,
    POSTGRES,
}

public class JDBCSink extends SinkBase {
    public static final String INSERT_TEMPLATE = "INSERT INTO %s (%s) VALUES (%s)";
    private static final String DELETE_TEMPLATE = "DELETE FROM %s WHERE %s";
    private static final String UPDATE_TEMPLATE = "UPDATE %s SET %s WHERE %s";
    private static final String ERROR_REPORT_TEMPLATE = "Error when exec %s, message %s";

    private final JDBCSinkConfig config;
    private final Connection conn;
    private final List<String> pkColumnNames;
    private final DatabaseType targetDbType;
    public static final String JDBC_COLUMN_NAME_KEY = "COLUMN_NAME";

    private String updateDeleteConditionBuffer;
    private Object[] updateDeleteValueBuffer;

    private static final Logger LOG = LoggerFactory.getLogger(JDBCSink.class);

    public JDBCSink(JDBCSinkConfig config, TableSchema tableSchema) {
        super(tableSchema);

        var jdbcUrl = config.getJdbcUrl().toLowerCase();
        if (jdbcUrl.startsWith("jdbc:mysql")) {
            this.targetDbType = DatabaseType.MYSQL;
        } else if (jdbcUrl.startsWith("jdbc:postgresql")) {
            this.targetDbType = DatabaseType.POSTGRES;
        } else {
            throw Status.INVALID_ARGUMENT
                    .withDescription("Unsupported jdbc url: " + jdbcUrl)
                    .asRuntimeException();
        }

        this.config = config;
        try {
            this.conn = DriverManager.getConnection(config.getJdbcUrl());
            this.conn.setAutoCommit(false);
            this.pkColumnNames = getPkColumnNames(conn, config.getTableName());
        } catch (SQLException e) {
            throw Status.INTERNAL
                    .withDescription(
                            String.format(ERROR_REPORT_TEMPLATE, e.getSQLState(), e.getMessage()))
                    .asRuntimeException();
        }
    }

    private static List<String> getPkColumnNames(Connection conn, String tableName) {
        List<String> pkColumnNames = new ArrayList<>();
        try {
            var pks = conn.getMetaData().getPrimaryKeys(null, null, tableName);
            while (pks.next()) {
                pkColumnNames.add(pks.getString(JDBC_COLUMN_NAME_KEY));
            }
        } catch (SQLException e) {
            throw Status.INTERNAL
                    .withDescription(
                            String.format(ERROR_REPORT_TEMPLATE, e.getSQLState(), e.getMessage()))
                    .asRuntimeException();
        }
        LOG.info("detected pk column {}", pkColumnNames);
        return pkColumnNames;
    }

    private PreparedStatement prepareStatement(SinkRow row) {
        switch (row.getOp()) {
            case INSERT:
                String columnsRepr = String.join(",", getTableSchema().getColumnNames());
                String valuesRepr =
                        IntStream.range(0, row.size())
                                .mapToObj(row::get)
                                .map((Object o) -> "?")
                                .collect(Collectors.joining(","));
                String insertStmt =
                        String.format(
                                INSERT_TEMPLATE, config.getTableName(), columnsRepr, valuesRepr);

                try {
                    return generatePreparedStatement(insertStmt, row, null);
                } catch (SQLException e) {
                    throw io.grpc.Status.INTERNAL
                            .withDescription(
                                    String.format(
                                            ERROR_REPORT_TEMPLATE, e.getSQLState(), e.getMessage()))
                            .withCause(e)
                            .asRuntimeException();
                }
            case DELETE:
                if (this.pkColumnNames.isEmpty()) {
                    throw Status.INTERNAL
                            .withDescription(
                                    "downstream jdbc table should have primary key to handle delete event")
                            .asRuntimeException();
                }
                String deleteCondition =
                        this.pkColumnNames.stream()
                                .map(key -> key + " = ?")
                                .collect(Collectors.joining(" AND "));

                String deleteStmt =
                        String.format(DELETE_TEMPLATE, config.getTableName(), deleteCondition);
                try {
                    int placeholderIdx = 1;
                    PreparedStatement stmt =
                            conn.prepareStatement(deleteStmt, Statement.RETURN_GENERATED_KEYS);
                    for (String primaryKey : this.pkColumnNames) {
                        Object fromRow = getTableSchema().getFromRow(primaryKey, row);
                        stmt.setObject(placeholderIdx++, fromRow);
                    }
                    return stmt;
                } catch (SQLException e) {
                    throw Status.INTERNAL
                            .withDescription(
                                    String.format(
                                            ERROR_REPORT_TEMPLATE, e.getSQLState(), e.getMessage()))
                            .asRuntimeException();
                }
            case UPDATE_DELETE:
                if (this.pkColumnNames.isEmpty()) {
                    throw Status.INTERNAL
                            .withDescription(
                                    "downstream jdbc table should have primary key to handle update_delete event")
                            .asRuntimeException();
                }
                updateDeleteConditionBuffer =
                        this.pkColumnNames.stream()
                                .map(key -> key + " = ?")
                                .collect(Collectors.joining(" AND "));
                updateDeleteValueBuffer =
                        this.pkColumnNames.stream()
                                .map(key -> getTableSchema().getFromRow(key, row))
                                .toArray();

                LOG.debug(
                        "update delete condition: {} on values {}",
                        updateDeleteConditionBuffer,
                        updateDeleteValueBuffer);
                return null;
            case UPDATE_INSERT:
                if (updateDeleteConditionBuffer == null) {
                    throw Status.FAILED_PRECONDITION
                            .withDescription("an UPDATE_INSERT should precede an UPDATE_DELETE")
                            .asRuntimeException();
                }
                String updateColumns =
                        IntStream.range(0, getTableSchema().getNumColumns())
                                .mapToObj(
                                        index -> getTableSchema().getColumnNames()[index] + " = ?")
                                .collect(Collectors.joining(","));
                String updateStmt =
                        String.format(
                                UPDATE_TEMPLATE,
                                config.getTableName(),
                                updateColumns,
                                updateDeleteConditionBuffer);
                try {
                    PreparedStatement stmt =
                            generatePreparedStatement(updateStmt, row, updateDeleteValueBuffer);
                    updateDeleteConditionBuffer = null;
                    updateDeleteValueBuffer = null;
                    return stmt;
                } catch (SQLException e) {
                    throw Status.INTERNAL
                            .withDescription(
                                    String.format(
                                            ERROR_REPORT_TEMPLATE, e.getSQLState(), e.getMessage()))
                            .asRuntimeException();
                }
            default:
                throw Status.INVALID_ARGUMENT
                        .withDescription("unspecified row operation")
                        .asRuntimeException();
        }
    }

    /**
     * Generates sql statement for insert/update
     *
     * @param inputStmt insert/update template string
     * @param row column values to fill into statement
     * @param updateDeleteValueBuffer pk values for update condition, pass null for insert
     * @return prepared sql statement for insert/delete
     * @throws SQLException
     */
    private PreparedStatement generatePreparedStatement(
            String inputStmt, SinkRow row, Object[] updateDeleteValueBuffer) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(inputStmt, Statement.RETURN_GENERATED_KEYS);
        var columnDescs = getTableSchema().getColumnDescs();
        int placeholderIdx = 1;
        for (int i = 0; i < row.size(); i++) {
            var column = columnDescs.get(i);
            switch (column.getDataType().getTypeName()) {
                case DECIMAL:
                    stmt.setBigDecimal(placeholderIdx++, (java.math.BigDecimal) row.get(i));
                    break;
                case INTERVAL:
                    if (targetDbType == DatabaseType.POSTGRES) {
                        stmt.setObject(placeholderIdx++, new PGInterval((String) row.get(i)));
                    } else {
                        stmt.setObject(placeholderIdx++, row.get(i));
                    }
                    break;
                case JSONB:
                    if (targetDbType == DatabaseType.POSTGRES) {
                        // reference: https://github.com/pgjdbc/pgjdbc/issues/265
                        var pgObj = new PGobject();
                        pgObj.setType("jsonb");
                        pgObj.setValue((String) row.get(i));
                        stmt.setObject(placeholderIdx++, pgObj);
                    } else {
                        stmt.setObject(placeholderIdx++, row.get(i));
                    }
                    break;
                case BYTEA:
                    stmt.setBinaryStream(placeholderIdx++, (InputStream) row.get(i));
                    break;
                case LIST:
                    var val = row.get(i);
                    // JSON payload returns a List, but JDBC expects an array
                    if (val instanceof java.util.List<?>) {
                        val = ((java.util.List<?>) val).toArray();
                    }
                    assert (val instanceof Object[]);
                    Object[] objArray = (Object[]) val;
                    if (targetDbType == DatabaseType.POSTGRES) {
                        var fieldType = column.getDataType().getFieldType(0);
                        var sqlType =
                                JdbcUtils.getDbSqlType(
                                        targetDbType, fieldType.getTypeName(), fieldType);
                        stmt.setArray(i + 1, conn.createArrayOf(sqlType, objArray));
                    } else {
                        // convert Array type to a string for other database
                        // reference:
                        // https://dev.mysql.com/doc/workbench/en/wb-migration-database-postgresql-typemapping.html
                        StringBuilder sb = new StringBuilder();
                        for (int j = 0; j < objArray.length; j++) {
                            sb.append('"').append(objArray[j].toString()).append('"');
                            if (j != objArray.length - 1) {
                                sb.append(",");
                            }
                        }
                        stmt.setString(placeholderIdx++, sb.toString());
                    }
                    break;
                default:
                    stmt.setObject(placeholderIdx++, row.get(i));
                    break;
            }
        }
        if (updateDeleteValueBuffer != null) {
            for (Object value : updateDeleteValueBuffer) {
                stmt.setObject(placeholderIdx++, value);
            }
        }
        return stmt;
    }

    @Override
    public void write(Iterator<SinkRow> rows) {
        while (rows.hasNext()) {
            try (SinkRow row = rows.next()) {
                PreparedStatement stmt = prepareStatement(row);
                if (row.getOp() == Data.Op.UPDATE_DELETE) {
                    continue;
                }
                if (stmt != null) {
                    try (stmt) {
                        LOG.debug("Executing statement: {}", stmt);
                        stmt.executeUpdate();
                    } catch (SQLException e) {
                        throw Status.INTERNAL
                                .withDescription(
                                        String.format(
                                                ERROR_REPORT_TEMPLATE,
                                                e.getSQLState(),
                                                e.getMessage()))
                                .asRuntimeException();
                    }
                } else {
                    throw Status.INTERNAL
                            .withDescription("empty statement encoded")
                            .asRuntimeException();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void sync() {
        if (updateDeleteConditionBuffer != null || updateDeleteValueBuffer != null) {
            throw Status.FAILED_PRECONDITION
                    .withDescription(
                            "expected UPDATE_INSERT to complete an UPDATE operation, got `sync`")
                    .asRuntimeException();
        }
        try {
            conn.commit();
        } catch (SQLException e) {
            throw io.grpc.Status.INTERNAL
                    .withDescription(
                            String.format(ERROR_REPORT_TEMPLATE, e.getSQLState(), e.getMessage()))
                    .asRuntimeException();
        }
    }

    @Override
    public void drop() {
        try {
            conn.close();
        } catch (SQLException e) {
            throw io.grpc.Status.INTERNAL
                    .withDescription(
                            String.format(ERROR_REPORT_TEMPLATE, e.getSQLState(), e.getMessage()))
                    .asRuntimeException();
        }
    }

    public String getTableName() {
        return config.getTableName();
    }

    public Connection getConn() {
        return conn;
    }
}
