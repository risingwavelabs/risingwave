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
import java.sql.*;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCSink extends SinkBase {
    public static final String INSERT_TEMPLATE = "INSERT INTO %s (%s) VALUES (%s)";
    private static final String DELETE_TEMPLATE = "DELETE FROM %s WHERE %s";
    private static final String UPDATE_TEMPLATE = "UPDATE %s SET %s WHERE %s";

    private final String tableName;
    private final Connection conn;
    private final String jdbcUrl;
    private final List<String> pkColumnNames;
    public static final String JDBC_COLUMN_NAME_KEY = "COLUMN_NAME";

    private String updateDeleteConditionBuffer;
    private Object[] updateDeleteValueBuffer;

    private static final Logger LOG = LoggerFactory.getLogger(JDBCSink.class);

    public JDBCSink(String tableName, String jdbcUrl, TableSchema tableSchema) {
        super(tableSchema);

        this.tableName = tableName;
        this.jdbcUrl = jdbcUrl;
        try {
            this.conn = DriverManager.getConnection(jdbcUrl);
            this.conn.setAutoCommit(false);
            this.pkColumnNames = getPkColumnNames(conn, tableName);
        } catch (SQLException e) {
            throw Status.INTERNAL.withCause(e).asRuntimeException();
        }
    }

    private List<String> getPkColumnNames(Connection conn, String tableName) {
        try {
            var pks = conn.getMetaData().getPrimaryKeys(null, null, tableName);
            while (pks.next()) {
                pkColumnNames.add(pks.getString(JDBC_COLUMN_NAME_KEY));
            }
        } catch (SQLException e) {
            throw Status.INTERNAL.withCause(e).asRuntimeException();
        }
        LOG.info("detected pk {}", this.pkColumnNames);
        return pkColumnNames;
    }

    public JDBCSink(Connection conn, TableSchema tableSchema, String tableName) {
        super(tableSchema);
        this.tableName = tableName;
        this.jdbcUrl = null;
        this.conn = conn;
        this.pkColumnNames = getPkColumnNames(conn, tableName);
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
                        String.format(INSERT_TEMPLATE, tableName, columnsRepr, valuesRepr);
                try {
                    PreparedStatement stmt =
                            conn.prepareStatement(insertStmt, Statement.RETURN_GENERATED_KEYS);
                    for (int i = 0; i < row.size(); i++) {
                        stmt.setObject(i + 1, row.get(i));
                    }
                    return stmt;
                } catch (SQLException e) {
                    throw io.grpc.Status.INTERNAL.withCause(e).asRuntimeException();
                }
            case DELETE:
                String deleteCondition;
                if (this.pkColumnNames.isEmpty()) {
                    deleteCondition =
                            IntStream.range(0, getTableSchema().getNumColumns())
                                    .mapToObj(
                                            index ->
                                                    getTableSchema().getColumnNames()[index]
                                                            + " = ?")
                                    .collect(Collectors.joining(" AND "));
                } else {
                    deleteCondition =
                            this.pkColumnNames.stream()
                                    .map(key -> key + " = ?")
                                    .collect(Collectors.joining(" AND "));
                }
                String deleteStmt = String.format(DELETE_TEMPLATE, tableName, deleteCondition);
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
                    throw Status.INTERNAL.withCause(e).asRuntimeException();
                }
            case UPDATE_DELETE:
                if (this.pkColumnNames.isEmpty()) {
                    updateDeleteConditionBuffer =
                            IntStream.range(0, getTableSchema().getNumColumns())
                                    .mapToObj(
                                            index ->
                                                    getTableSchema().getColumnNames()[index]
                                                            + " = ?")
                                    .collect(Collectors.joining(" AND "));
                    updateDeleteValueBuffer =
                            IntStream.range(0, getTableSchema().getNumColumns())
                                    .mapToObj(
                                            index ->
                                                    getTableSchema()
                                                            .getFromRow(
                                                                    getTableSchema()
                                                                            .getColumnNames()[
                                                                            index],
                                                                    row))
                                    .toArray();
                } else {
                    updateDeleteConditionBuffer =
                            this.pkColumnNames.stream()
                                    .map(key -> key + " = ?")
                                    .collect(Collectors.joining(" AND "));
                    updateDeleteValueBuffer =
                            this.pkColumnNames.stream()
                                    .map(key -> getTableSchema().getFromRow(key, row))
                                    .toArray();
                }
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
                                tableName,
                                updateColumns,
                                updateDeleteConditionBuffer);
                try {
                    PreparedStatement stmt =
                            conn.prepareStatement(updateStmt, Statement.RETURN_GENERATED_KEYS);
                    int placeholderIdx = 1;
                    for (int i = 0; i < row.size(); i++) {
                        stmt.setObject(placeholderIdx++, row.get(i));
                    }
                    for (Object value : updateDeleteValueBuffer) {
                        stmt.setObject(placeholderIdx++, value);
                    }
                    updateDeleteConditionBuffer = null;
                    updateDeleteValueBuffer = null;
                    return stmt;
                } catch (SQLException e) {
                    throw Status.INTERNAL.withCause(e).asRuntimeException();
                }
            default:
                throw Status.INVALID_ARGUMENT
                        .withDescription("unspecified row operation")
                        .asRuntimeException();
        }
    }

    @Override
    public void write(Iterator<SinkRow> rows) {
        while (rows.hasNext()) {
            SinkRow row = rows.next();
            PreparedStatement stmt = prepareStatement(row);
            if (row.getOp() == Data.Op.UPDATE_DELETE) {
                continue;
            }
            if (stmt != null) {
                try {
                    LOG.debug("Executing statement: " + stmt);
                    stmt.executeUpdate();
                } catch (SQLException e) {
                    throw Status.INTERNAL.withCause(e).asRuntimeException();
                }
            } else {
                throw Status.INTERNAL
                        .withDescription("empty statement encoded")
                        .asRuntimeException();
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
            throw io.grpc.Status.INTERNAL.withCause(e).asRuntimeException();
        }
    }

    @Override
    public void drop() {
        try {
            conn.close();
        } catch (SQLException e) {
            throw io.grpc.Status.INTERNAL.withCause(e).asRuntimeException();
        }
    }

    public String getTableName() {
        return tableName;
    }
}
