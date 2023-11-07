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
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.connector.api.sink.SinkWriterBase;
import com.risingwave.connector.jdbc.JdbcDialect;
import com.risingwave.proto.Data;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.grpc.Status;
import java.sql.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCSink extends SinkWriterBase {
    private static final String ERROR_REPORT_TEMPLATE = "Error when exec %s, message %s";

    private final JdbcDialect jdbcDialect;
    private final JDBCSinkConfig config;
    private final HikariDataSource connPool;
    private final List<String> pkColumnNames;

    public static final String JDBC_COLUMN_NAME_KEY = "COLUMN_NAME";
    public static final String JDBC_DATA_TYPE_KEY = "DATA_TYPE";

    private boolean updateFlag = false;

    private static final Logger LOG = LoggerFactory.getLogger(JDBCSink.class);

    public JDBCSink(JDBCSinkConfig config, TableSchema tableSchema) {
        super(tableSchema);

        var jdbcUrl = config.getJdbcUrl().toLowerCase();
        var factory = JdbcUtils.getDialectFactory(jdbcUrl);
        this.config = config;
        try {
            HikariConfig conf = new HikariConfig();
            conf.setJdbcUrl(config.getJdbcUrl());

            // The connection pool manages only one connection for its sink executor,
            // we use the pool to keep the connection alive.
            conf.setPoolName(String.format("hikari-pool-%s", config.getTableName()));
            conf.setMaximumPoolSize(1);
            conf.setMinimumIdle(1);
            // disable auto commit can improve performance
            conf.setAutoCommit(false);
            // explicitly set isolation level to RC
            conf.setTransactionIsolation("TRANSACTION_READ_COMMITTED");
            connPool = new HikariDataSource(conf);

            try (var conn = connPool.getConnection()) {
                // Retrieve primary keys and column type mappings from the database
                this.pkColumnNames =
                        getPkColumnNames(conn, config.getTableName(), config.getSchemaName());
                // column name -> java.sql.Types
                Map<String, Integer> columnTypeMapping =
                        getColumnTypeMapping(conn, config.getTableName(), config.getSchemaName());
                // create an array that each slot corresponding to each column in TableSchema
                var columnSqlTypes = new int[tableSchema.getNumColumns()];
                for (int columnIdx = 0; columnIdx < tableSchema.getNumColumns(); columnIdx++) {
                    var columnName = tableSchema.getColumnNames()[columnIdx];
                    columnSqlTypes[columnIdx] = columnTypeMapping.get(columnName);
                }
                LOG.info("columnSqlTypes: {}", Arrays.toString(columnSqlTypes));

                if (factory.isPresent()) {
                    this.jdbcDialect = factory.get().create(columnSqlTypes);
                } else {
                    throw Status.INVALID_ARGUMENT
                            .withDescription("Unsupported jdbc url: " + jdbcUrl)
                            .asRuntimeException();
                }
            }
        } catch (SQLException e) {
            throw Status.INTERNAL
                    .withDescription(
                            String.format(ERROR_REPORT_TEMPLATE, e.getSQLState(), e.getMessage()))
                    .asRuntimeException();
        }
    }

    private static Map<String, Integer> getColumnTypeMapping(
            Connection conn, String tableName, String schemaName) {
        Map<String, Integer> columnTypeMap = new HashMap<>();
        try {
            ResultSet columnResultSet =
                    conn.getMetaData().getColumns(null, schemaName, tableName, null);

            while (columnResultSet.next()) {
                columnTypeMap.put(
                        columnResultSet.getString(JDBC_COLUMN_NAME_KEY),
                        columnResultSet.getInt(JDBC_DATA_TYPE_KEY));
            }
        } catch (SQLException e) {
            throw Status.INTERNAL
                    .withDescription(
                            String.format(ERROR_REPORT_TEMPLATE, e.getSQLState(), e.getMessage()))
                    .asRuntimeException();
        }
        LOG.info("detected column type mapping {}", columnTypeMap);
        return columnTypeMap;
    }

    private static List<String> getPkColumnNames(
            Connection conn, String tableName, String schemaName) {
        List<String> pkColumnNames = new ArrayList<>();
        try {
            var pks = conn.getMetaData().getPrimaryKeys(null, schemaName, tableName);
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

    @Override
    public void write(Iterator<SinkRow> rows) {
        try (var conn = connPool.getConnection();
                var jdbcStatements = new JdbcStatements(conn)) {

            // fill prepare statements with parameters
            while (rows.hasNext()) {
                SinkRow row = rows.next();
                if (row.getOp() == Data.Op.UPDATE_DELETE) {
                    updateFlag = true;
                    continue;
                }
                if (config.isUpsertSink()) {
                    if (row.getOp() == Data.Op.DELETE) {
                        jdbcStatements.prepareDelete(row);
                    } else {
                        jdbcStatements.prepareUpsert(row);
                    }
                } else {
                    jdbcStatements.prepareInsert(row);
                }
            }

            // Execute staging statements after all rows are prepared.
            jdbcStatements.execute();
        } catch (SQLException e) {
            throw Status.INTERNAL
                    .withDescription(
                            String.format(ERROR_REPORT_TEMPLATE, e.getSQLState(), e.getMessage()))
                    .asRuntimeException();
        }
    }

    /** A wrapper class for JDBC prepared statements */
    class JdbcStatements implements AutoCloseable {
        private PreparedStatement deleteStatement;
        private PreparedStatement upsertStatement;
        private PreparedStatement insertStatement;

        private final Connection conn;

        public JdbcStatements(Connection conn) throws SQLException {
            this.conn = conn;
            var schemaTableName =
                    jdbcDialect.createSchemaTableName(
                            config.getSchemaName(), config.getTableName());

            if (config.isUpsertSink()) {
                var upsertSql =
                        jdbcDialect.getUpsertStatement(
                                schemaTableName,
                                List.of(getTableSchema().getColumnNames()),
                                pkColumnNames);
                // MySQL and Postgres have upsert SQL
                if (upsertSql.isEmpty()) {
                    throw Status.FAILED_PRECONDITION
                            .withDescription("Failed to get upsert SQL")
                            .asRuntimeException();
                }

                this.upsertStatement =
                        conn.prepareStatement(upsertSql.get(), Statement.RETURN_GENERATED_KEYS);
                // upsert sink will handle DELETE events
                var deleteSql = jdbcDialect.getDeleteStatement(schemaTableName, pkColumnNames);
                this.deleteStatement =
                        conn.prepareStatement(deleteSql, Statement.RETURN_GENERATED_KEYS);
            } else {
                var insertSql =
                        jdbcDialect.getInsertIntoStatement(
                                schemaTableName, List.of(getTableSchema().getColumnNames()));
                this.insertStatement =
                        conn.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS);
            }
        }

        public void prepareUpsert(SinkRow row) {
            try {
                switch (row.getOp()) {
                    case INSERT:
                        jdbcDialect.bindUpsertStatement(
                                upsertStatement, conn, getTableSchema(), row);
                        break;
                    case UPDATE_INSERT:
                        if (!updateFlag) {
                            throw Status.FAILED_PRECONDITION
                                    .withDescription(
                                            "an UPDATE_DELETE should precede an UPDATE_INSERT")
                                    .asRuntimeException();
                        }
                        jdbcDialect.bindUpsertStatement(
                                upsertStatement, conn, getTableSchema(), row);
                        updateFlag = false;
                        break;
                    default:
                        throw Status.FAILED_PRECONDITION
                                .withDescription("unexpected op type: " + row.getOp())
                                .asRuntimeException();
                }
                upsertStatement.addBatch();
            } catch (SQLException e) {
                throw io.grpc.Status.INTERNAL
                        .withDescription(
                                String.format(
                                        ERROR_REPORT_TEMPLATE, e.getSQLState(), e.getMessage()))
                        .withCause(e)
                        .asRuntimeException();
            }
        }

        public void prepareDelete(SinkRow row) {
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
                    deleteStatement.setObject(placeholderIdx++, fromRow);
                }
                deleteStatement.addBatch();
            } catch (SQLException e) {
                throw Status.INTERNAL
                        .withDescription(
                                String.format(
                                        ERROR_REPORT_TEMPLATE, e.getSQLState(), e.getMessage()))
                        .asRuntimeException();
            }
        }

        public void prepareInsert(SinkRow row) {
            if (row.getOp() != Data.Op.INSERT) {
                throw Status.FAILED_PRECONDITION
                        .withDescription("unexpected op type: " + row.getOp())
                        .asRuntimeException();
            }
            try {
                jdbcDialect.bindInsertIntoStatement(insertStatement, conn, getTableSchema(), row);
                insertStatement.addBatch();
            } catch (SQLException e) {
                throw io.grpc.Status.INTERNAL
                        .withDescription(
                                String.format(
                                        ERROR_REPORT_TEMPLATE, e.getSQLState(), e.getMessage()))
                        .withCause(e)
                        .asRuntimeException();
            }
        }

        public void execute() throws SQLException {
            // We execute DELETE statement before to avoid accidentally deletion.
            executeStatement(this.deleteStatement);
            executeStatement(this.upsertStatement);
            executeStatement(this.insertStatement);

            this.conn.commit();
        }

        @Override
        public void close() {
            closeStatement(this.deleteStatement);
            closeStatement(this.upsertStatement);
            closeStatement(this.insertStatement);
        }

        private void executeStatement(PreparedStatement stmt) throws SQLException {
            if (stmt == null) {
                return;
            }
            LOG.debug("Executing statement: {}", stmt);
            stmt.executeBatch();
            stmt.clearParameters();
        }

        private void closeStatement(PreparedStatement stmt) {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                LOG.error("unable to close the prepared stmt", e);
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
    }

    @Override
    public void drop() {
        if (connPool != null) {
            connPool.close();
        }
    }

    public String getTableName() {
        return config.getTableName();
    }

    public HikariDataSource getConnPool() {
        return connPool;
    }
}
