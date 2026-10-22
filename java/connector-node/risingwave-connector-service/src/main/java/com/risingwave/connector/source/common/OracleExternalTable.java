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
import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.java.binding.Binding;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.Data;
import com.risingwave.proto.Data.DataType.TypeName;
import com.risingwave.proto.PlanCommon;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientConnectionException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Pull-based Oracle catalog and live table scan operations used by the Rust CDC backfill. */
final class OracleExternalTable {
    private static final Logger LOG = LoggerFactory.getLogger(OracleExternalTable.class);
    private static final int BATCH_ROW_COUNT = 256;
    private static final BufferAllocator ROOT_ALLOCATOR = new RootAllocator();

    private OracleExternalTable() {}

    static Session openSession(Map<String, String> properties) {
        return new Session(properties);
    }

    /**
     * An Oracle connection shared by the current-SCN queries and snapshot pages of one Rust
     * external table reader. The connection is opened lazily and reopened after a connection
     * failure.
     */
    static final class Session implements AutoCloseable {
        private final Map<String, String> properties;
        private Connection connection;

        private Session(Map<String, String> properties) {
            this.properties = Map.copyOf(properties);
        }

        synchronized long currentScn() throws SQLException, IOException {
            return execute(OracleExternalTable::currentScn, () -> true);
        }

        synchronized void snapshotRead(
                ConnectorServiceProto.OracleSnapshotReadRequest request,
                List<Object> startPk,
                long channelPtr)
                throws SQLException, IOException {
            var delivered = new boolean[] {false};
            execute(
                    connection -> {
                        OracleExternalTable.snapshotRead(
                                connection, request, startPk, channelPtr, delivered);
                        return null;
                    },
                    () -> !delivered[0]);
        }

        @Override
        public synchronized void close() {
            closeConnection();
        }

        private <T> T execute(ConnectionOperation<T> operation, BooleanSupplier canRetry)
                throws SQLException, IOException {
            try {
                return operation.apply(connection());
            } catch (SQLException error) {
                if (!isConnectionFailure(error)) {
                    throw error;
                }
                closeConnection();
                if (!canRetry.getAsBoolean()) {
                    throw error;
                }
                LOG.warn("Oracle connection failed; retrying once on a new connection", error);
                try {
                    return operation.apply(connection());
                } catch (SQLException retryError) {
                    if (isConnectionFailure(retryError)) {
                        closeConnection();
                    }
                    throw retryError;
                }
            }
        }

        private Connection connection() throws SQLException {
            if (connection == null || connection.isClosed()) {
                connection = connect(properties);
            }
            return connection;
        }

        private void closeConnection() {
            if (connection == null) {
                return;
            }
            try {
                connection.close();
            } catch (SQLException error) {
                LOG.warn("Failed to close Oracle connection", error);
            }
            connection = null;
        }

        private static boolean isConnectionFailure(SQLException error) {
            var state = error.getSQLState();
            return error instanceof SQLRecoverableException
                    || error instanceof SQLTransientConnectionException
                    || error instanceof SQLNonTransientConnectionException
                    || (state != null && state.startsWith("08"));
        }
    }

    @FunctionalInterface
    private interface ConnectionOperation<T> {
        T apply(Connection connection) throws SQLException, IOException;
    }

    static ConnectorServiceProto.TableSchema discover(
            ConnectorServiceProto.OracleConnectionProperties properties) throws SQLException {
        var propertiesMap = properties.getPropertiesMap();
        try (var connection = connect(propertiesMap)) {
            var schemaName = propertiesMap.get(DbzConnectorConfig.ORACLE_SCHEMA_NAME);
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

    private static long currentScn(Connection connection) throws SQLException {
        try (var statement = connection.createStatement();
                var result = statement.executeQuery("SELECT CURRENT_SCN FROM V$DATABASE")) {
            if (!result.next()) {
                throw new SQLException(
                        "Oracle returned no row for SELECT CURRENT_SCN FROM V$DATABASE");
            }
            var scn = result.getLong(1);
            if (scn <= 0) {
                throw new SQLException("Oracle returned an invalid current SCN: " + scn);
            }
            return scn;
        }
    }

    private static void snapshotRead(
            Connection connection,
            ConnectorServiceProto.OracleSnapshotReadRequest request,
            List<Object> startPk,
            long channelPtr,
            boolean[] delivered)
            throws SQLException, IOException {
        if (!request.hasTableSchema()) {
            throw new SQLException(
                    "Oracle snapshot request is missing its RisingWave table schema");
        }
        if (request.getLimit() <= 0) {
            throw new SQLException("Oracle snapshot request has an invalid limit");
        }

        var schemaName = request.getSchemaName();
        var tableName = request.getTableName();
        var tableSchema = request.getTableSchema();
        var columnNames =
                tableSchema.getColumnsList().stream().map(PlanCommon.ColumnDesc::getName).toList();
        validatePrimaryKeys(request, tableSchema, startPk);
        var sql =
                buildSnapshotSql(
                        columnNames,
                        schemaName,
                        tableName,
                        request.getPrimaryKeysList(),
                        !startPk.isEmpty(),
                        request.getLimit());

        try (var statement = connection.prepareStatement(sql);
                var allocator =
                        ROOT_ALLOCATOR.newChildAllocator("oracle-snapshot", 0, Long.MAX_VALUE);
                var writer = new OracleArrowBatchWriter(allocator, tableSchema)) {
            if (!startPk.isEmpty()) {
                bindStartPrimaryKey(statement, startPk);
            }
            try (var result = statement.executeQuery()) {
                while (result.next()) {
                    writer.append(result);
                    if (writer.rowCount() == BATCH_ROW_COUNT
                            && !sendBatch(writer, channelPtr, delivered)) {
                        return;
                    }
                }
            }
            if (writer.rowCount() > 0) {
                sendBatch(writer, channelPtr, delivered);
            }
        }
    }

    private static boolean sendBatch(
            OracleArrowBatchWriter writer, long channelPtr, boolean[] delivered)
            throws IOException {
        var response =
                ConnectorServiceProto.OracleSnapshotReadResponse.newBuilder()
                        .setArrowIpc(ByteString.copyFrom(writer.finishBatch()))
                        .build();
        delivered[0] = true;
        return Binding.sendOracleSnapshotResponseToChannel(channelPtr, response.toByteArray());
    }

    private static Connection connect(Map<String, String> properties) throws SQLException {
        var pdbName =
                OracleValidator.normalizeIdentifierInConfigOption(
                        DbzConnectorConfig.ORACLE_PDB_NAME,
                        properties.get(DbzConnectorConfig.ORACLE_PDB_NAME));
        var jdbcUrl =
                ValidatorUtils.getJdbcUrl(
                        SourceTypeE.ORACLE,
                        properties.get(DbzConnectorConfig.HOST),
                        properties.get(DbzConnectorConfig.PORT),
                        OracleValidator.normalizeIdentifierInConfigOption(
                                DbzConnectorConfig.DB_NAME,
                                properties.get(DbzConnectorConfig.DB_NAME)));
        var connection =
                DriverManager.getConnection(
                        jdbcUrl,
                        properties.get(DbzConnectorConfig.USER),
                        properties.get(DbzConnectorConfig.PASSWORD));
        try (var statement = connection.createStatement()) {
            statement.execute(
                    "ALTER SESSION SET CONTAINER = " + OracleValidator.quoteIdentifier(pdbName));
            statement.execute("ALTER SESSION SET TIME_ZONE = 'UTC'");
            // Match RisingWave's native string ordering for keyset pagination.
            statement.execute("ALTER SESSION SET NLS_SORT = BINARY");
            statement.execute("ALTER SESSION SET NLS_COMP = BINARY");
        } catch (SQLException error) {
            connection.close();
            throw error;
        }
        return connection;
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

    private static List<String> discoverPrimaryKeys(
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
                                + "WHERE cons.OWNER = ? AND cons.TABLE_NAME = ? "
                                + "AND cons.CONSTRAINT_TYPE = 'P' AND cons.STATUS = 'ENABLED' "
                                + "ORDER BY cols.POSITION")) {
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
     * ordering during PK-ordered snapshot pagination. Otherwise, the page frontier could skip or
     * repeat rows.
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

    private static Data.DataType oracleTypeToRisingWaveType(
            String rawType, Integer precision, Integer scale) throws SQLException {
        var oracleType = rawType.toUpperCase(Locale.ROOT);
        var typeName =
                switch (oracleType) {
                    case "BINARY_FLOAT" -> TypeName.FLOAT;
                    case "BINARY_DOUBLE" -> TypeName.DOUBLE;
                        // Oracle FLOAT and its aliases are NUMBER subtypes with binary precision,
                        // not
                        // IEEE-754 values. Preserve their decimal semantics.
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
                                    "LONG",
                                    "ROWID",
                                    "UROWID",
                                    "XMLTYPE" ->
                            TypeName.VARCHAR;
                    case "RAW", "LONG RAW", "BLOB" -> TypeName.BYTEA;
                    case "BOOLEAN" -> TypeName.BOOLEAN;
                    case "JSON" -> TypeName.JSONB;
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
            List<String> columns,
            String schemaName,
            String tableName,
            List<String> primaryKeys,
            boolean hasStartPrimaryKey,
            int limit)
            throws SQLException {
        if (columns.isEmpty() || primaryKeys.isEmpty()) {
            throw new SQLException("Oracle snapshot query requires columns and primary keys");
        }
        if (limit <= 0) {
            throw new SQLException("Oracle snapshot query has an invalid limit");
        }
        var fields = columns.stream().map(OracleValidator::quoteIdentifier).toList();
        var keys = primaryKeys.stream().map(OracleValidator::quoteIdentifier).toList();
        var sql =
                new StringBuilder("SELECT ")
                        .append(String.join(",", fields))
                        .append(" FROM ")
                        .append(OracleValidator.quoteIdentifier(schemaName))
                        .append(".")
                        .append(OracleValidator.quoteIdentifier(tableName));
        if (hasStartPrimaryKey) {
            sql.append(" WHERE ").append(buildPrimaryKeyFilter(keys));
        }
        return sql.append(" ORDER BY ")
                .append(String.join(",", keys))
                .append(" FETCH FIRST ")
                .append(limit)
                .append(" ROWS ONLY")
                .toString();
    }

    private static String buildPrimaryKeyFilter(List<String> quotedPrimaryKeys) {
        var disjunction = new ArrayList<String>();
        for (int greaterIndex = 0; greaterIndex < quotedPrimaryKeys.size(); greaterIndex++) {
            var conjunction = new ArrayList<String>();
            for (int equalIndex = 0; equalIndex < greaterIndex; equalIndex++) {
                conjunction.add(quotedPrimaryKeys.get(equalIndex) + " = ?");
            }
            conjunction.add(quotedPrimaryKeys.get(greaterIndex) + " > ?");
            disjunction.add("(" + String.join(" AND ", conjunction) + ")");
        }
        return String.join(" OR ", disjunction);
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

    private static void bindStartPrimaryKey(PreparedStatement statement, List<Object> startPk)
            throws SQLException {
        var parameterIndex = 1;
        for (int greaterIndex = 0; greaterIndex < startPk.size(); greaterIndex++) {
            for (int keyIndex = 0; keyIndex <= greaterIndex; keyIndex++) {
                statement.setObject(parameterIndex++, startPk.get(keyIndex));
            }
        }
    }

    private static Integer nullableInteger(ResultSet result, String column) throws SQLException {
        var value = result.getInt(column);
        return result.wasNull() ? null : value;
    }

    private record OracleColumn(String name, Data.DataType dataType, boolean nullable) {}
}
