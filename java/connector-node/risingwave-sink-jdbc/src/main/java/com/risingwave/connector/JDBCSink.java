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
import com.risingwave.connector.jdbc.JdbcDialectFactory;
import com.risingwave.proto.Data;
import io.grpc.Status;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCSink extends SinkBase {
    private static final String ERROR_REPORT_TEMPLATE = "Error when exec %s, message %s";

    private final JdbcDialect jdbcDialect;
    private final JDBCSinkConfig config;
    private final Connection conn;
    private final List<String> pkColumnNames;
    public static final String JDBC_COLUMN_NAME_KEY = "COLUMN_NAME";

    private final PreparedStatement upsertPreparedStmt;
    private PreparedStatement deletePreparedStmt;

    private boolean updateFlag = false;
    private static final Logger LOG = LoggerFactory.getLogger(JDBCSink.class);

    public JDBCSink(JDBCSinkConfig config, TableSchema tableSchema) {
        super(tableSchema);

        var jdbcUrl = config.getJdbcUrl().toLowerCase();
        var factory = JdbcUtils.getDialectFactory(jdbcUrl);
        this.jdbcDialect =
                factory.map(JdbcDialectFactory::create)
                        .orElseThrow(
                                () ->
                                        Status.INVALID_ARGUMENT
                                                .withDescription("Unsupported jdbc url: " + jdbcUrl)
                                                .asRuntimeException());
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

    private PreparedStatement prepareUpsertStatement(SinkRow row) {
        try {
            switch (row.getOp()) {
                case INSERT:
                    jdbcDialect.bindUpsertStatement(
                            upsertPreparedStmt, conn, getTableSchema(), row);
                    return upsertPreparedStmt;
                case UPDATE_INSERT:
                    if (!updateFlag) {
                        throw Status.FAILED_PRECONDITION
                                .withDescription("an UPDATE_DELETE should precede an UPDATE_INSERT")
                                .asRuntimeException();
                    }
                    jdbcDialect.bindUpsertStatement(
                            upsertPreparedStmt, conn, getTableSchema(), row);
                    updateFlag = false;
                    return upsertPreparedStmt;
                default:
                    throw Status.FAILED_PRECONDITION
                            .withDescription("unexpected op type: " + row.getOp())
                            .asRuntimeException();
            }
        } catch (SQLException e) {
            throw io.grpc.Status.INTERNAL
                    .withDescription(
                            String.format(ERROR_REPORT_TEMPLATE, e.getSQLState(), e.getMessage()))
                    .withCause(e)
                    .asRuntimeException();
        }
    }

    private PreparedStatement prepareDeleteStatement(SinkRow row) {
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
            return deletePreparedStmt;
        } catch (SQLException e) {
            throw Status.INTERNAL
                    .withDescription(
                            String.format(ERROR_REPORT_TEMPLATE, e.getSQLState(), e.getMessage()))
                    .asRuntimeException();
        }
    }

    @Override
    public void write(Iterator<SinkRow> rows) {
        while (rows.hasNext()) {
            try (SinkRow row = rows.next()) {
                PreparedStatement stmt;
                if (row.getOp() == Data.Op.UPDATE_DELETE) {
                    updateFlag = true;
                    continue;
                }

                if (row.getOp() == Data.Op.DELETE) {
                    stmt = prepareDeleteStatement(row);
                } else {
                    stmt = prepareUpsertStatement(row);
                }

                try {
                    LOG.debug("Executing statement: {}", stmt);
                    stmt.executeUpdate();
                    stmt.clearParameters();
                } catch (SQLException e) {
                    throw Status.INTERNAL
                            .withDescription(
                                    String.format(ERROR_REPORT_TEMPLATE, stmt, e.getMessage()))
                            .asRuntimeException();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void sync() {
        if (updateFlag) {
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
            upsertPreparedStmt.close();
            deletePreparedStmt.close();
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
