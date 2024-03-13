// Copyright 2024 RisingWave Labs
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
import com.risingwave.connector.api.sink.SinkWriter;
import com.risingwave.connector.jdbc.JdbcDialect;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.Data;
import io.grpc.Status;
import java.sql.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCSink implements SinkWriter {
    private static final String ERROR_REPORT_TEMPLATE = "Error when exec %s, message %s";

    private final JdbcDialect jdbcDialect;
    private final JDBCSinkConfig config;
    private Connection conn;
    private JdbcStatements jdbcStatements;
    private final List<String> pkColumnNames;

    private final TableSchema tableSchema;

    public static final String JDBC_COLUMN_NAME_KEY = "COLUMN_NAME";
    public static final String JDBC_DATA_TYPE_KEY = "DATA_TYPE";

    private boolean updateFlag = false;

    private static final Logger LOG = LoggerFactory.getLogger(JDBCSink.class);

    public JDBCSink(JDBCSinkConfig config, TableSchema tableSchema) {
        this.tableSchema = tableSchema;

        var jdbcUrl = config.getJdbcUrl().toLowerCase();
        var factory = JdbcUtils.getDialectFactory(jdbcUrl);
        this.config = config;
        try {
            conn = JdbcUtils.getConnection(config.getJdbcUrl());
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
            LOG.info(
                    "schema = {}, table = {}: columnSqlTypes = {}",
                    config.getSchemaName(),
                    config.getTableName(),
                    Arrays.toString(columnSqlTypes));

            if (factory.isPresent()) {
                this.jdbcDialect = factory.get().create(columnSqlTypes);
            } else {
                throw Status.INVALID_ARGUMENT
                        .withDescription("Unsupported jdbc url: " + jdbcUrl)
                        .asRuntimeException();
            }
            LOG.info(
                    "JDBC connection: autoCommit = {}, trxn = {}",
                    conn.getAutoCommit(),
                    conn.getTransactionIsolation());

            jdbcStatements = new JdbcStatements(conn);
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
        LOG.info(
                "schema = {}, table = {}: detected column type mapping = {}",
                schemaName,
                tableName,
                columnTypeMap);
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
        LOG.info(
                "schema = {}, table = {}: detected pk column = {}",
                schemaName,
                tableName,
                pkColumnNames);
        return pkColumnNames;
    }

    @Override
    public boolean write(Iterable<SinkRow> rows) {
        final int maxRetryCount = 4;
        int retryCount = 0;
        while (true) {
            try {
                // fill prepare statements with parameters
                for (SinkRow row : rows) {
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
                break;
            } catch (SQLException e) {
                LOG.error("Failed to execute JDBC statements, retried {} times", retryCount, e);
                if (++retryCount > maxRetryCount) {
                    throw Status.INTERNAL
                            .withDescription(
                                    "Failed to execute JDBC statements and exceeded max retry times")
                            .withCause(e)
                            .asRuntimeException();
                }

                try {
                    if (!conn.isValid(10)) { // 10 seconds timeout
                        LOG.info("Recreate the JDBC connection due to connection broken");
                        // close the statements and connection first
                        jdbcStatements.close();
                        conn.close();

                        // create a new connection if the current connection is invalid
                        conn = JdbcUtils.getConnection(config.getJdbcUrl());
                        // reset the flag since we will retry to prepare the batch again
                        updateFlag = false;
                        jdbcStatements = new JdbcStatements(conn);
                    } else {
                        throw io.grpc.Status.INTERNAL
                                .withDescription(
                                        String.format(
                                                ERROR_REPORT_TEMPLATE,
                                                e.getSQLState(),
                                                e.getMessage()))
                                .asRuntimeException();
                    }
                } catch (SQLException ex) {
                    LOG.error(
                            "Failed to create a new JDBC connection, retried {} times",
                            retryCount,
                            ex);
                    throw io.grpc.Status.INTERNAL
                            .withDescription(
                                    String.format(
                                            ERROR_REPORT_TEMPLATE,
                                            ex.getSQLState(),
                                            ex.getMessage()))
                            .asRuntimeException();
                }
            }
        }
        return true;
    }

    /**
     * A wrapper class for JDBC prepared statements We create this object one time and reuse it
     * across multiple batches if only the JDBC connection is valid.
     */
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
                                List.of(tableSchema.getColumnNames()),
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
                                schemaTableName, List.of(tableSchema.getColumnNames()));
                this.insertStatement =
                        conn.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS);
            }
        }

        public void prepareUpsert(SinkRow row) {
            try {
                switch (row.getOp()) {
                    case INSERT:
                        jdbcDialect.bindUpsertStatement(upsertStatement, conn, tableSchema, row);
                        break;
                    case UPDATE_INSERT:
                        if (!updateFlag) {
                            throw Status.FAILED_PRECONDITION
                                    .withDescription(
                                            "an UPDATE_DELETE should precede an UPDATE_INSERT")
                                    .asRuntimeException();
                        }
                        jdbcDialect.bindUpsertStatement(upsertStatement, conn, tableSchema, row);
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
                    Object fromRow = tableSchema.getFromRow(primaryKey, row);
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
                jdbcDialect.bindInsertIntoStatement(insertStatement, conn, tableSchema, row);
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
            // we don't close the connection here, because we don't own it
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
    public void beginEpoch(long epoch) {}

    @Override
    public Optional<ConnectorServiceProto.SinkMetadata> barrier(boolean isCheckpoint) {
        if (updateFlag) {
            throw Status.FAILED_PRECONDITION
                    .withDescription(
                            "expected UPDATE_INSERT to complete an UPDATE operation, got `sync`")
                    .asRuntimeException();
        }
        return Optional.empty();
    }

    @Override
    public void drop() {
        if (jdbcStatements != null) {
            jdbcStatements.close();
        }

        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            LOG.error("unable to close conn: %s", e);
        }
    }

    public String getTableName() {
        return config.getTableName();
    }

    public Connection getConn() {
        return conn;
    }
}
