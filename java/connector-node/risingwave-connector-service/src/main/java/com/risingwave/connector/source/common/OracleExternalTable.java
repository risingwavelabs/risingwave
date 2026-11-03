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

import com.google.protobuf.ByteString;
import com.risingwave.java.binding.Binding;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.Data;
import com.risingwave.proto.Data.DataType.TypeName;
import com.risingwave.proto.PlanCommon;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import oracle.ucp.UniversalConnectionPoolAdapter;
import oracle.ucp.UniversalConnectionPoolException;
import oracle.ucp.admin.UniversalConnectionPoolManagerImpl;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Pull-based Oracle catalog and live table scan operations used by the Rust CDC backfill. */
final class OracleExternalTable {
    private static final Logger LOG = LoggerFactory.getLogger(OracleExternalTable.class);
    private static final int BATCH_ROW_COUNT = 256;
    private static final BufferAllocator ROOT_ALLOCATOR = new RootAllocator();
    private static final int MAX_POOL_SIZE = 2;
    private static final int CONNECTION_WAIT_TIMEOUT_SECONDS = 60;
    private static final int INACTIVE_CONNECTION_TIMEOUT_SECONDS = 300;
    // Must be longer than the slowest legitimate single socket read, such as a query's first fetch.
    private static final long READ_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(10);
    private static final AtomicLong NEXT_POOL_ID = new AtomicLong(1);

    private OracleExternalTable() {}

    static PoolDataSource openPool(Map<String, String> properties)
            throws SQLException, UniversalConnectionPoolException {
        var pdbName = pdbName(properties);
        var pool = PoolDataSourceFactory.getPoolDataSource();
        pool.setConnectionPoolName("risingwave-oracle-" + NEXT_POOL_ID.getAndIncrement());
        pool.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
        pool.setURL(jdbcUrl(properties));
        pool.setUser(properties.get(DbzConnectorConfig.USER));
        pool.setPassword(properties.get(DbzConnectorConfig.PASSWORD));
        pool.setConnectionProperty("oracle.jdbc.ReadTimeout", String.valueOf(READ_TIMEOUT_MILLIS));
        pool.setInitialPoolSize(0);
        pool.setMinPoolSize(0);
        pool.setMaxPoolSize(MAX_POOL_SIZE);
        pool.setConnectionWaitTimeout(CONNECTION_WAIT_TIMEOUT_SECONDS);
        pool.setInactiveConnectionTimeout(INACTIVE_CONNECTION_TIMEOUT_SECONDS);
        pool.setValidateConnectionOnBorrow(true);
        pool.registerConnectionInitializationCallback(
                connection -> initializeSession(connection, pdbName));
        UniversalConnectionPoolManagerImpl.getUniversalConnectionPoolManager()
                .createConnectionPool((UniversalConnectionPoolAdapter) pool);
        return pool;
    }

    static void closePool(PoolDataSource pool) throws UniversalConnectionPoolException {
        UniversalConnectionPoolManagerImpl.getUniversalConnectionPoolManager()
                .destroyConnectionPool(pool.getConnectionPoolName());
    }

    /**
     * Cancels the Oracle query of one JNI call from a Rust external table reader, such as {@link
     * #snapshotRead} or {@link #currentScn}, terminating the call that runs it on a Tokio blocking
     * thread. A cancel before the query starts makes the call return without running it.
     */
    static final class OracleStatementCancel {
        private final String table;
        private final String query;
        private volatile boolean cancelled;
        private Statement statement;

        OracleStatementCancel(String table, String query) {
            this.table = table;
            this.query = query;
        }

        void cancel() {
            if (cancelled) {
                return;
            }
            synchronized (this) {
                if (cancelled) {
                    return;
                }
                cancelled = true;
                // Null: the call has not registered yet, or it already ended and ran unregister().
                if (statement == null) {
                    LOG.info(
                            "Oracle {} of {}: cancel requested, no query is running", query, table);
                    return;
                }
                LOG.info("Oracle {} of {}: cancel requested, interrupting the query", query, table);
                try {
                    statement.cancel();
                    LOG.info("Oracle {} of {}: query interrupted", query, table);
                } catch (SQLException error) {
                    LOG.warn("Oracle {} of {}: failed to interrupt the query", query, table, error);
                }
            }
        }

        boolean isCancelled() {
            return cancelled;
        }

        private synchronized boolean register(Statement statement) {
            if (cancelled) {
                return false;
            }
            this.statement = statement;
            return true;
        }

        private synchronized void unregister() {
            statement = null;
        }
    }

    static ConnectorServiceProto.TableSchema discover(
            ConnectorServiceProto.OracleConnectionProperties properties) throws SQLException {
        var propertiesMap = properties.getPropertiesMap();
        try (var connection =
                DriverManager.getConnection(
                        jdbcUrl(propertiesMap),
                        propertiesMap.get(DbzConnectorConfig.USER),
                        propertiesMap.get(DbzConnectorConfig.PASSWORD))) {
            initializeSession(connection, pdbName(propertiesMap));
            var schemaName = propertiesMap.get(DbzConnectorConfig.SCHEMA_NAME);
            var tableName = propertiesMap.get(DbzConnectorConfig.TABLE_NAME);
            var columns = discoverColumns(connection, schemaName, tableName);
            if (columns.isEmpty()) {
                throw new SQLException(
                        String.format(
                                "Oracle table '%s.%s' has no visible columns",
                                schemaName, tableName));
            }

            var columnIndices = new HashMap<String, Integer>();
            var tableSchema = ConnectorServiceProto.TableSchema.newBuilder();
            for (int i = 0; i < columns.size(); i++) {
                var column = columns.get(i);
                columnIndices.put(column.name(), i);
                tableSchema.addColumns(
                        PlanCommon.ColumnDesc.newBuilder()
                                .setName(column.name())
                                .setColumnType(column.dataType())
                                .setNullable(column.nullable())
                                .build());
            }

            for (var primaryKey : discoverPrimaryKeys(connection, schemaName, tableName)) {
                var index = columnIndices.get(primaryKey);
                if (index == null) {
                    throw new SQLException(
                            String.format(
                                    "Oracle primary-key column '%s' is not a visible table column",
                                    primaryKey));
                }
                tableSchema.addPkIndices(index);
            }
            if (tableSchema.getPkIndicesCount() == 0) {
                throw new SQLException(
                        String.format(
                                "Oracle table '%s.%s' doesn't define a primary key",
                                schemaName, tableName));
            }

            return tableSchema.build();
        }
    }

    static long currentScn(PoolDataSource pool, OracleStatementCancel cancel) throws SQLException {
        if (cancel.isCancelled()) {
            throw new SQLException("Oracle current SCN query was cancelled");
        }
        try (var connection = pool.getConnection();
                var statement = connection.createStatement()) {
            if (!cancel.register(statement)) {
                throw new SQLException("Oracle current SCN query was cancelled");
            }
            try (var result = statement.executeQuery("SELECT CURRENT_SCN FROM V$DATABASE")) {
                if (!result.next()) {
                    throw new SQLException(
                            "Oracle returned no row for SELECT CURRENT_SCN FROM V$DATABASE");
                }
                var scn = result.getLong(1);
                if (scn <= 0) {
                    throw new SQLException("Oracle returned an invalid current SCN: " + scn);
                }
                return scn;
            } finally {
                cancel.unregister();
            }
        }
    }

    static void snapshotRead(
            PoolDataSource pool,
            ConnectorServiceProto.OracleSnapshotReadRequest request,
            List<Object> startPk,
            long channelPtr,
            OracleStatementCancel cancel)
            throws SQLException, IOException {
        if (cancel.isCancelled()) {
            return;
        }
        if (!request.hasTableSchema()) {
            throw new SQLException(
                    "Oracle snapshot request is missing its RisingWave table schema");
        }
        var schemaName = request.getSchemaName();
        var tableName = request.getTableName();
        var tableSchema = request.getTableSchema();
        var primaryKeys = request.getPrimaryKeysList();
        validatePrimaryKeys(request, tableSchema, startPk);

        try (var connection = pool.getConnection();
                var allocator =
                        ROOT_ALLOCATOR.newChildAllocator("oracle-snapshot", 0, Long.MAX_VALUE);
                var writer = new OracleArrowBatchWriter(allocator, tableSchema)) {
            var minBoundCount = startPk.isEmpty() ? 0 : 1;
            for (int boundCount = startPk.size(); boundCount >= minBoundCount; boundCount--) {
                var sql =
                        buildSnapshotSql(
                                tableSchema.getColumnsList(),
                                schemaName,
                                tableName,
                                primaryKeys,
                                boundCount);
                if (!readKeyRange(
                        connection,
                        sql,
                        startPk.subList(0, boundCount),
                        writer,
                        channelPtr,
                        cancel)) {
                    return;
                }
            }
            if (writer.rowCount() > 0) {
                sendBatch(writer, channelPtr);
            }
        }
    }

    private static boolean readKeyRange(
            Connection connection,
            String sql,
            List<Object> bounds,
            OracleArrowBatchWriter writer,
            long channelPtr,
            OracleStatementCancel cancel)
            throws SQLException, IOException {
        try (var statement = connection.prepareStatement(sql)) {
            if (!cancel.register(statement)) {
                return false;
            }
            try {
                statement.setFetchSize(BATCH_ROW_COUNT);
                for (int i = 0; i < bounds.size(); i++) {
                    statement.setObject(i + 1, bounds.get(i));
                }
                if (cancel.isCancelled()) {
                    return false;
                }
                // A cancel landing after this check still ends the query: the first sendBatch()
                // fails because the receiver is already gone.
                try (var result = statement.executeQuery()) {
                    while (result.next()) {
                        writer.append(result);
                        if (writer.rowCount() == BATCH_ROW_COUNT
                                && !sendBatch(writer, channelPtr)) {
                            return false;
                        }
                    }
                }
                return true;
            } catch (SQLException error) {
                if (cancel.isCancelled()) {
                    return false;
                }
                throw error;
            } finally {
                cancel.unregister();
            }
        }
    }

    private static boolean sendBatch(OracleArrowBatchWriter writer, long channelPtr)
            throws IOException {
        var response =
                ConnectorServiceProto.OracleSnapshotReadResponse.newBuilder()
                        .setArrowIpc(ByteString.copyFrom(writer.finishBatch()))
                        .build();
        return Binding.sendOracleSnapshotResponseToChannel(channelPtr, response.toByteArray());
    }

    private static String pdbName(Map<String, String> properties) throws SQLException {
        return OracleValidator.normalizeIdentifierInConfigOption(
                OracleDbzConnectorConfig.PDB_NAME,
                properties.get(OracleDbzConnectorConfig.PDB_NAME));
    }

    private static String jdbcUrl(Map<String, String> properties) throws SQLException {
        return OracleCdcSource.getJdbcUrl(
                properties.get(DbzConnectorConfig.HOST),
                properties.get(DbzConnectorConfig.PORT),
                OracleValidator.normalizeIdentifierInConfigOption(
                        DbzConnectorConfig.DB_NAME, properties.get(DbzConnectorConfig.DB_NAME)));
    }

    private static void initializeSession(Connection connection, String pdbName)
            throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.execute(
                    "ALTER SESSION SET CONTAINER = " + OracleValidator.quoteIdentifier(pdbName));
            // Match RisingWave's native string ordering for keyset pagination.
            statement.execute(
                    "ALTER SESSION SET TIME_ZONE = 'UTC' NLS_SORT = BINARY NLS_COMP = BINARY");
        }
    }

    private static List<OracleColumn> discoverColumns(
            Connection connection, String schemaName, String tableName) throws SQLException {
        var columns = new ArrayList<OracleColumn>();
        try (var statement =
                connection.prepareStatement(
                        "SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE, NULLABLE "
                                + "FROM ALL_TAB_COLS "
                                + "WHERE OWNER = ? AND TABLE_NAME = ? AND HIDDEN_COLUMN = 'NO' "
                                + "ORDER BY COLUMN_ID")) {
            statement.setString(1, schemaName);
            statement.setString(2, tableName);
            try (var result = statement.executeQuery()) {
                while (result.next()) {
                    var precision = nullableInteger(result, "DATA_PRECISION");
                    var scale = nullableInteger(result, "DATA_SCALE");
                    columns.add(
                            new OracleColumn(
                                    result.getString("COLUMN_NAME"),
                                    oracleTypeToRisingWaveType(
                                            result.getString("DATA_TYPE"), precision, scale),
                                    "Y".equals(result.getString("NULLABLE"))));
                }
            }
        }
        return columns;
    }

    static List<String> discoverPrimaryKeys(
            Connection connection, String schemaName, String tableName) throws SQLException {
        var primaryKeys = new ArrayList<String>();
        try (var statement =
                connection.prepareStatement(
                        "SELECT cols.COLUMN_NAME, tc.DATA_TYPE, tc.DATA_PRECISION, tc.DATA_SCALE, tc.COLLATION "
                                + "FROM ALL_CONSTRAINTS cons "
                                + "JOIN ALL_CONS_COLUMNS cols "
                                + "ON cons.OWNER = cols.OWNER "
                                + "AND cons.CONSTRAINT_NAME = cols.CONSTRAINT_NAME "
                                + "JOIN ALL_TAB_COLS tc ON tc.OWNER = cols.OWNER "
                                + "AND tc.TABLE_NAME = cols.TABLE_NAME AND tc.COLUMN_NAME = cols.COLUMN_NAME "
                                + "JOIN ALL_IND_COLUMNS ic ON ic.INDEX_OWNER = cons.INDEX_OWNER "
                                + "AND ic.INDEX_NAME = cons.INDEX_NAME AND ic.COLUMN_NAME = cols.COLUMN_NAME "
                                + "WHERE cons.OWNER = ? AND cons.TABLE_NAME = ? "
                                + "AND cons.CONSTRAINT_TYPE = 'P' AND cons.STATUS = 'ENABLED' "
                                + "ORDER BY ic.COLUMN_POSITION")) {
            statement.setString(1, schemaName);
            statement.setString(2, tableName);
            try (var result = statement.executeQuery()) {
                while (result.next()) {
                    validatePrimaryKeyOrdering(connection, result);
                    primaryKeys.add(result.getString("COLUMN_NAME"));
                }
            }
        }
        return primaryKeys;
    }

    /**
     * Checks that an Oracle primary-key column can be decoded and compared without changing its
     * ordering during PK-ordered snapshot reads. Otherwise, a read resumed from the last emitted
     * key could skip or repeat rows.
     *
     * <p>Accepts NUMBER with declared precision, scale, and integer width each fitting within
     * RisingWave's 28-digit decimal; RAW; DATE; TIMESTAMP with fractional precision at most 6; and
     * CHAR/VARCHAR2 with BINARY or USING_NLS_COMP collation in an AL32UTF8 database. Snapshot
     * sessions set NLS_COMP and NLS_SORT to BINARY for the session-controlled collation case.
     */
    private static void validatePrimaryKeyOrdering(Connection connection, ResultSet column)
            throws SQLException {
        var name = column.getString("COLUMN_NAME");
        var type = column.getString("DATA_TYPE");
        if (type.equals("NUMBER")) {
            // Rust's decimal representation has 28 decimal digits and a maximum scale of 28.
            // Rounding a key would collapse distinct rows and change the pagination frontier.
            var precision = nullableInteger(column, "DATA_PRECISION");
            var scale = nullableInteger(column, "DATA_SCALE");
            if (precision != null
                    && scale != null
                    && precision <= 28
                    && scale <= 28
                    && precision - scale <= 28) {
                return;
            }
            throw new SQLException(
                    "Oracle CDC numeric primary key '"
                            + name
                            + "' requires a declared NUMBER precision, scale and integer width "
                            + "representable within 28 decimal digits");
        }
        if (type.equals("RAW") || type.equals("DATE")) {
            return;
        }
        if (type.startsWith("TIMESTAMP")) {
            var scale = nullableInteger(column, "DATA_SCALE");
            if (scale != null && scale <= 6) {
                return;
            }
        }
        if (type.equals("VARCHAR2") || type.equals("CHAR")) {
            // A linguistic column collation overrides the binary session settings. National
            // character types also need a different ordering from RisingWave's UTF-8 strings.
            var collation = column.getString("COLLATION");
            if (collation != null
                    && !collation.equals("BINARY")
                    && !collation.equals("USING_NLS_COMP")) {
                throw new SQLException(
                        "Oracle CDC primary key '" + name + "' requires binary collation");
            }
            try (var statement = connection.createStatement();
                    var result =
                            statement.executeQuery(
                                    "SELECT VALUE FROM NLS_DATABASE_PARAMETERS "
                                            + "WHERE PARAMETER = 'NLS_CHARACTERSET'")) {
                if (result.next() && result.getString(1).equals("AL32UTF8")) {
                    return;
                }
            }
            throw new SQLException(
                    "Oracle CDC character primary keys require the AL32UTF8 database character set");
        }
        throw new SQLException(
                "Oracle CDC primary key '"
                        + name
                        + "' has unsupported type "
                        + type
                        + "; use NUMBER, RAW, DATE, TIMESTAMP with precision <= 6, "
                        + "or binary CHAR/VARCHAR2 in an AL32UTF8 database");
    }

    static Data.DataType oracleTypeToRisingWaveType(
            String rawType, Integer precision, Integer scale) throws SQLException {
        var oracleType = rawType.toUpperCase(Locale.ROOT);
        var typeName =
                switch (oracleType) {
                    case "BINARY_FLOAT" -> TypeName.FLOAT;
                    case "BINARY_DOUBLE" -> TypeName.DOUBLE;
                        // Oracle FLOAT and its aliases are NUMBER subtypes with binary
                        // precision, not IEEE-754 values. Preserve their decimal semantics.
                    case "FLOAT", "REAL", "DOUBLE PRECISION" -> TypeName.DECIMAL;
                    case "NUMBER", "DECIMAL", "NUMERIC", "INTEGER", "INT", "SMALLINT" ->
                            oracleNumberType(precision, scale);
                    case "CHAR",
                                    "NCHAR",
                                    "VARCHAR",
                                    "VARCHAR2",
                                    "NVARCHAR2",
                                    "CLOB",
                                    "NCLOB",
                                    "ROWID" ->
                            TypeName.VARCHAR;
                    case "RAW", "BLOB" -> TypeName.BYTEA;
                    case "DATE" -> TypeName.TIMESTAMP;
                    default -> {
                        if (oracleType.startsWith("TIMESTAMP")) {
                            yield oracleType.contains("TIME ZONE")
                                    ? TypeName.TIMESTAMPTZ
                                    : TypeName.TIMESTAMP;
                        }
                        throw new SQLException("Unsupported Oracle data type: " + rawType);
                    }
                };
        return Data.DataType.newBuilder().setTypeName(typeName).build();
    }

    private static TypeName oracleNumberType(Integer precision, Integer scale) {
        if (precision == null || precision <= 0 || scale == null || scale > 0) {
            return TypeName.DECIMAL;
        }
        var integerDigits = precision - scale;
        if (integerDigits < 5) {
            return TypeName.INT16;
        }
        if (integerDigits < 10) {
            return TypeName.INT32;
        }
        if (integerDigits < 19) {
            return TypeName.INT64;
        }
        return TypeName.DECIMAL;
    }

    private static String buildSnapshotSql(
            List<PlanCommon.ColumnDesc> columns,
            String schemaName,
            String tableName,
            List<String> primaryKeys,
            int boundCount)
            throws SQLException {
        if (columns.isEmpty() || primaryKeys.isEmpty()) {
            throw new SQLException("Oracle snapshot query requires columns and primary keys");
        }
        var fields =
                columns.stream()
                        .map(
                                column -> {
                                    var field = OracleValidator.quoteIdentifier(column.getName());
                                    return column.getColumnType().getTypeName()
                                                    == TypeName.TIMESTAMPTZ
                                            ? "SYS_EXTRACT_UTC(" + field + ")"
                                            : field;
                                })
                        .toList();
        var keys = primaryKeys.stream().map(OracleValidator::quoteIdentifier).toList();
        // FIRST_ROWS makes Oracle prefer a plan that reads rows through the PK index, which is kept
        // in PK order, so rows are produced only as fast as Rust consumes them. Without it, the
        // default ALL_ROWS goal can pick a full table scan, which reads blocks in physical order
        // rather than PK order
        // (https://docs.oracle.com/en/database/oracle/oracle-database/23/tgsql/optimizer-access-paths.html#GUID-461E7071-2229-4F60-82E6-BC4F6FC8D23B),
        // and then sorts the rest of the table by PK before returning the first row. Rust consumes
        // this query under back pressure and cancels it at the next snapshot stream rebuild, so
        // most of that sort would be wasted, and every rebuild would repeat it, which makes the
        // performance very bad. INDEX_ASC pins the PK index, and the key range predicate below
        // has no OR, so the index range scan starts exactly at the resume key and each read costs
        // the same however far the snapshot has progressed.
        var sql =
                new StringBuilder("SELECT /*+ FIRST_ROWS(")
                        .append(BATCH_ROW_COUNT)
                        .append(") INDEX_ASC(t (")
                        .append(String.join(" ", keys))
                        .append(")) */ ")
                        .append(String.join(",", fields))
                        .append(" FROM ")
                        .append(OracleValidator.quoteIdentifier(schemaName))
                        .append(".")
                        .append(OracleValidator.quoteIdentifier(tableName))
                        .append(" t");
        if (boundCount > 0) {
            var conditions = new ArrayList<String>();
            for (int i = 0; i < boundCount - 1; i++) {
                conditions.add(keys.get(i) + " = ?");
            }
            conditions.add(keys.get(boundCount - 1) + " > ?");
            sql.append(" WHERE ").append(String.join(" AND ", conditions));
        }
        return sql.append(" ORDER BY ").append(String.join(",", keys)).toString();
    }

    private static void validatePrimaryKeys(
            ConnectorServiceProto.OracleSnapshotReadRequest request,
            ConnectorServiceProto.TableSchema tableSchema,
            List<Object> startPk)
            throws SQLException {
        if (request.getPrimaryKeysCount() != tableSchema.getPkIndicesCount()) {
            throw new SQLException(
                    "Oracle snapshot primary-key names and indices have different lengths");
        }
        if (!startPk.isEmpty() && startPk.size() != request.getPrimaryKeysCount()) {
            throw new SQLException("Oracle snapshot start key has the wrong number of values");
        }
        if (startPk.stream().anyMatch(Objects::isNull)) {
            throw new SQLException(
                    "Oracle snapshot primary-key value cannot be NULL or a non-finite decimal");
        }
        for (int i = 0; i < request.getPrimaryKeysCount(); i++) {
            var schemaIndex = tableSchema.getPkIndices(i);
            if (schemaIndex >= tableSchema.getColumnsCount()) {
                throw new SQLException("Oracle snapshot primary-key index is outside the schema");
            }
            var schemaName = tableSchema.getColumns(schemaIndex).getName();
            if (!request.getPrimaryKeys(i).equals(schemaName)) {
                throw new SQLException(
                        String.format(
                                "Oracle snapshot primary key '%s' does not match schema column '%s'",
                                request.getPrimaryKeys(i), schemaName));
            }
        }
    }

    static Integer nullableInteger(ResultSet result, String column) throws SQLException {
        var value = result.getInt(column);
        return result.wasNull() ? null : value;
    }

    private record OracleColumn(String name, Data.DataType dataType, boolean nullable) {}
}
