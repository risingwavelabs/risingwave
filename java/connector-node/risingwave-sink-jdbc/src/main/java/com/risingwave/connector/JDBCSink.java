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
import com.risingwave.connector.jdbc.JdbcDialect;
import com.risingwave.connector.jdbc.PostgresDialect;
import com.risingwave.proto.Data;
import io.grpc.Status;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.StringUtils;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

enum DatabaseType {
    MYSQL,
    POSTGRES,
}

public class JDBCSink extends SinkBase {
    private static final String DELETE_TEMPLATE = "DELETE FROM %s WHERE %s";
    private static final String UPDATE_TEMPLATE = "UPDATE %s SET %s WHERE %s";
    private static final String ERROR_REPORT_TEMPLATE = "Error when exec %s, message %s";

    private final JdbcDialect jdbcDialect;
    private final JDBCSinkConfig config;
    private final Connection conn;
    private final List<String> pkColumnNames;
    public static final String JDBC_COLUMN_NAME_KEY = "COLUMN_NAME";

    private final PreparedStatement upsertPreparedStmt;
    private PreparedStatement deletePreparedStmt;

    private String updateDeleteConditionBuffer;
    private Object[] updateDeleteValueBuffer;

    private boolean updateFlag = false;
    private static final Logger LOG = LoggerFactory.getLogger(JDBCSink.class);

    public JDBCSink(JDBCSinkConfig config, TableSchema tableSchema) {
        super(tableSchema);

        var jdbcUrl = config.getJdbcUrl().toLowerCase();
        var factory = JdbcUtils.getDialectFactory(jdbcUrl);
        if (factory.isEmpty()) {
            throw Status.INVALID_ARGUMENT
                    .withDescription("Unsupported jdbc url: " + jdbcUrl)
                    .asRuntimeException();
        }
        this.jdbcDialect = factory.get().create();
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

        // Upsert sink must have primary key
        if (config.isUpsertSink() && pkColumnNames.isEmpty()) {
            throw Status.FAILED_PRECONDITION
                    .withDescription(
                            "No primary key in downstream for table " + config.getTableName())
                    .asRuntimeException();
        }

        try {
            var upsertSql =
                    jdbcDialect.getUpsertStatement(
                            config.getTableName(),
                            List.of(tableSchema.getColumnNames()),
                            pkColumnNames);
            // MySQL and Postgres have upsert SQL
            assert (upsertSql.isPresent());
            this.upsertPreparedStmt =
                    conn.prepareStatement(upsertSql.get(), Statement.RETURN_GENERATED_KEYS);
            // upsert sink will handle DELETE events
            if (config.isUpsertSink()) {
                var deleteSql =
                        jdbcDialect.getDeleteStatement(config.getTableName(), pkColumnNames);
                this.deletePreparedStmt =
                        conn.prepareStatement(deleteSql, Statement.RETURN_GENERATED_KEYS);
            }
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

    private Optional<PreparedStatement> prepareUpsertStatement(SinkRow row) {
        try {
            switch (row.getOp()) {
                case INSERT:
                    bindUpsertStatementValues(upsertPreparedStmt, row);
                    return Optional.of(upsertPreparedStmt);
                case UPDATE_DELETE:
                    updateFlag = true;
                    return Optional.empty();
                case UPDATE_INSERT:
                    if (!updateFlag) {
                        throw Status.FAILED_PRECONDITION
                                .withDescription("an UPDATE_DELETE should precede an UPDATE_INSERT")
                                .asRuntimeException();
                    }
                    bindUpsertStatementValues(upsertPreparedStmt, row);
                    updateFlag = false;
                    return Optional.of(upsertPreparedStmt);
                default:
                    return Optional.empty();
            }
        } catch (SQLException e) {
            throw io.grpc.Status.INTERNAL
                    .withDescription(
                            String.format(ERROR_REPORT_TEMPLATE, e.getSQLState(), e.getMessage()))
                    .withCause(e)
                    .asRuntimeException();
        }
    }

    private Optional<PreparedStatement> prepareDeleteStatement(SinkRow row) {
        assert row.getOp() == Data.Op.DELETE;
        if (!config.isUpsertSink()) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("Non-upsert sink cannot handle DELETE event")
                    .asRuntimeException();
        }
        if (pkColumnNames.isEmpty()) {
            throw Status.INTERNAL
                    .withDescription(
                            "downstream jdbc table should have primary key to handle DELETE event")
                    .asRuntimeException();
        }

        try {
            int placeholderIdx = 1;
            for (String primaryKey : pkColumnNames) {
                Object fromRow = getTableSchema().getFromRow(primaryKey, row);
                deletePreparedStmt.setObject(placeholderIdx++, fromRow);
            }
            return Optional.of(deletePreparedStmt);
        } catch (SQLException e) {
            throw Status.INTERNAL
                    .withDescription(
                            String.format(ERROR_REPORT_TEMPLATE, e.getSQLState(), e.getMessage()))
                    .asRuntimeException();
        }
    }

    // private PreparedStatement prepareUpdateOrDeleteStatement(SinkRow row) {
    //     switch (row.getOp()) {
    //         case DELETE:
    //             if (this.pkColumnNames.isEmpty()) {
    //                 throw Status.INTERNAL
    //                         .withDescription(
    //                                 "downstream jdbc table should have primary key to handle delete event")
    //                         .asRuntimeException();
    //             }
    //             String deleteCondition =
    //                     this.pkColumnNames.stream()
    //                             .map(key -> key + " = ?")
    //                             .collect(Collectors.joining(" AND "));
    //
    //             String deleteStmt =
    //                     String.format(DELETE_TEMPLATE, config.getTableName(), deleteCondition);
    //             try {
    //                 int placeholderIdx = 1;
    //                 PreparedStatement stmt =
    //                         conn.prepareStatement(deleteStmt, Statement.RETURN_GENERATED_KEYS);
    //                 for (String primaryKey : this.pkColumnNames) {
    //                     Object fromRow = getTableSchema().getFromRow(primaryKey, row);
    //                     stmt.setObject(placeholderIdx++, fromRow);
    //                 }
    //                 return stmt;
    //             } catch (SQLException e) {
    //                 throw Status.INTERNAL
    //                         .withDescription(
    //                                 String.format(
    //                                         ERROR_REPORT_TEMPLATE, e.getSQLState(), e.getMessage()))
    //                         .asRuntimeException();
    //             }
    //         case UPDATE_DELETE:
    //             if (this.pkColumnNames.isEmpty()) {
    //                 throw Status.INTERNAL
    //                         .withDescription(
    //                                 "downstream jdbc table should have primary key to handle update_delete event")
    //                         .asRuntimeException();
    //             }
    //             updateDeleteConditionBuffer =
    //                     this.pkColumnNames.stream()
    //                             .map(key -> key + " = ?")
    //                             .collect(Collectors.joining(" AND "));
    //             updateDeleteValueBuffer =
    //                     this.pkColumnNames.stream()
    //                             .map(key -> getTableSchema().getFromRow(key, row))
    //                             .toArray();
    //
    //             LOG.debug(
    //                     "update delete condition: {} on values {}",
    //                     updateDeleteConditionBuffer,
    //                     updateDeleteValueBuffer);
    //             return null;
    //         case UPDATE_INSERT:
    //             if (updateDeleteConditionBuffer == null) {
    //                 throw Status.FAILED_PRECONDITION
    //                         .withDescription("an UPDATE_INSERT should precede an UPDATE_DELETE")
    //                         .asRuntimeException();
    //             }
    //             String updateColumns =
    //                     IntStream.range(0, getTableSchema().getNumColumns())
    //                             .mapToObj(
    //                                     index -> getTableSchema().getColumnNames()[index] + " = ?")
    //                             .collect(Collectors.joining(","));
    //             String updateStmt =
    //                     String.format(
    //                             UPDATE_TEMPLATE,
    //                             config.getTableName(),
    //                             updateColumns,
    //                             updateDeleteConditionBuffer);
    //             try {
    //                 PreparedStatement stmt =
    //                         conn.prepareStatement(updateStmt, Statement.RETURN_GENERATED_KEYS);
    //                 bindUpsertStatementValues(stmt, row, updateDeleteValueBuffer);
    //                 updateDeleteConditionBuffer = null;
    //                 updateDeleteValueBuffer = null;
    //                 return stmt;
    //             } catch (SQLException e) {
    //                 throw Status.INTERNAL
    //                         .withDescription(
    //                                 String.format(
    //                                         ERROR_REPORT_TEMPLATE, e.getSQLState(), e.getMessage()))
    //                         .asRuntimeException();
    //             }
    //         default:
    //             throw Status.INTERNAL
    //                     .withDescription("enter unreachable code")
    //                     .asRuntimeException();
    //     }
    // }

    /**
     * Bind prepared statement parameters for insert/update command.
     *
     * @param stmt prepared statement for
     * @param row column values to fill into statement
     * @return prepared sql statement for insert/delete
     * @throws SQLException
     */
    private void bindUpsertStatementValues(PreparedStatement stmt, SinkRow row)
            throws SQLException {
        var columnDescs = getTableSchema().getColumnDescs();
        int placeholderIdx = 1;
        for (int i = 0; i < row.size(); i++) {
            var column = columnDescs.get(i);
            switch (column.getDataType().getTypeName()) {
                case DECIMAL:
                    stmt.setBigDecimal(placeholderIdx++, (java.math.BigDecimal) row.get(i));
                    break;
                case INTERVAL:
                    if (jdbcDialect instanceof PostgresDialect) {
                        stmt.setObject(placeholderIdx++, new PGInterval((String) row.get(i)));
                    } else {
                        stmt.setObject(placeholderIdx++, row.get(i));
                    }
                    break;
                case JSONB:
                    if (jdbcDialect instanceof PostgresDialect) {
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
                    stmt.setBytes(placeholderIdx++, (byte[]) row.get(i));
                    break;
                case LIST:
                    var val = row.get(i);
                    assert (val instanceof Object[]);
                    Object[] objArray = (Object[]) val;
                    if (jdbcDialect instanceof PostgresDialect) {
                        assert (column.getDataType().getFieldTypeCount() == 1);
                        var fieldType = column.getDataType().getFieldType(0);
                        stmt.setArray(
                                i + 1,
                                conn.createArrayOf(fieldType.getTypeName().name(), objArray));
                    } else {
                        // convert Array type to a string for other database
                        // reference:
                        // https://dev.mysql.com/doc/workbench/en/wb-migration-database-postgresql-typemapping.html
                        var arrayString = StringUtils.join(objArray, ",");
                        stmt.setString(placeholderIdx++, arrayString);
                    }
                    break;
                default:
                    stmt.setObject(placeholderIdx++, row.get(i));
                    break;
            }
        }
    }

    @Override
    public void write(Iterator<SinkRow> rows) {
        // set parameters for each row
        // and don't close the statement until the sink is closed
        while (rows.hasNext()) {
            try (SinkRow row = rows.next()) {
                Optional<PreparedStatement> stmt;
                if (row.getOp() != Data.Op.DELETE) {
                    stmt = prepareUpsertStatement(row);
                } else {
                    stmt = prepareDeleteStatement(row);
                }
                if (row.getOp() == Data.Op.UPDATE_DELETE) {
                    continue;
                }
                if (stmt.isPresent()) {
                    try {
                        LOG.debug("Executing statement: {}", stmt.get());
                        stmt.get().executeUpdate();
                        stmt.get().clearParameters();
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
            updateDeleteConditionBuffer = null;
            updateDeleteValueBuffer = null;
            upsertPreparedStmt.close();
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
