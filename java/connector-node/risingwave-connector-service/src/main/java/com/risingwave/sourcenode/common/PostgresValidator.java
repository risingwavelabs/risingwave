package com.risingwave.sourcenode.common;

import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.Data;
import io.grpc.Status;
import io.grpc.StatusException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresValidator implements AutoCloseable {
    static final Logger LOG = LoggerFactory.getLogger(PostgresValidator.class);

    private final Properties sqlStmts;
    private final Map<String, String> props;
    // todo: refactor to the class in connector-api
    private final ConnectorServiceProto.TableSchema tableSchema;
    private final Connection conn;

    public PostgresValidator(
            String jdbcUrl,
            String dbUser,
            String dbPassword,
            Map<String, String> userPros,
            Properties sqlStmts,
            ConnectorServiceProto.TableSchema tableSchema)
            throws SQLException {
        this.sqlStmts = sqlStmts;
        this.tableSchema = tableSchema;
        this.props = userPros;
        this.conn = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword);
    }

    public void validateAll() throws Exception {
        validateLogConfig();
        validateTableSchema();
        validatePrivileges();
    }

    public void validateDistributedTable() throws Exception {
        String schemaName = props.get(DbzConnectorConfig.PG_SCHEMA_NAME);
        String tableName = props.get(DbzConnectorConfig.TABLE_NAME);
        try (var stmt = conn.prepareStatement(sqlStmts.getProperty("citus.distributed_table"))) {
            stmt.setString(1, schemaName + "." + tableName);
            var res = stmt.executeQuery();
            while (res.next()) {
                var ret = res.getString(1);
                if (!ret.equalsIgnoreCase("distributed")) {
                    throw new RuntimeException("Citus table is not a distributed table");
                }
            }
        }
    }

    public void validateTableSchema() throws Exception {
        String schemaName = props.get(DbzConnectorConfig.PG_SCHEMA_NAME);
        String tableName = props.get(DbzConnectorConfig.TABLE_NAME);

        try (var stmt = conn.prepareStatement(sqlStmts.getProperty("postgres.table"))) {
            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);
            var res = stmt.executeQuery();
            while (res.next()) {
                var ret = res.getString(1);
                if (ret.equalsIgnoreCase("f") || ret.equalsIgnoreCase("false")) {
                    throw new RuntimeException("Postgres table or schema doesn't exist");
                }
            }
        }
        // check primary key
        // reference: https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
        try (var stmt = conn.prepareStatement(sqlStmts.getProperty("postgres.pk"))) {
            stmt.setString(1, schemaName + "." + tableName);
            var res = stmt.executeQuery();
            var pkFields = new HashSet<String>();
            while (res.next()) {
                var name = res.getString(1);
                pkFields.add(name);
            }

            if (!isPkMatch(tableSchema, pkFields)) {
                throw new RuntimeException("Primary key mismatch");
            }
        }
        // check whether source schema match table schema on upstream
        try (var stmt = conn.prepareStatement(sqlStmts.getProperty("postgres.table_schema"))) {
            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);
            var res = stmt.executeQuery();
            int index = 0;
            while (res.next()) {
                var field = res.getString(1);
                var dataType = res.getString(2);
                if (index >= tableSchema.getColumnsCount()) {
                    throw new RuntimeException("The number of columns mismatch");
                }
                var srcCol = tableSchema.getColumns(index++);
                if (!srcCol.getName().equals(field)) {
                    throw new RuntimeException(
                            String.format(
                                    "table column defined in the source mismatches upstream column %s",
                                    field));
                }
                if (!isPostgresDataTypeCompatible(dataType, srcCol.getDataType())) {
                    throw new RuntimeException(
                            String.format("incompatible data type of column %s", srcCol.getName()));
                }
            }
        }
    }

    public void validateLogConfig() throws Exception {
        // check whether source db has enabled wal
        try (var stmt = conn.createStatement()) {
            var res = stmt.executeQuery(sqlStmts.getProperty("postgres.wal"));
            while (res.next()) {
                if (!res.getString(1).equals("logical")) {
                    throw new StatusException(
                            Status.INTERNAL.withDescription(
                                    "Postgres wal_level should be 'logical'.\nPlease modify the config and restart your Postgres server."));
                }
            }
        }
    }

    public void validatePrivileges() throws Exception {
        boolean isSuperUser = false;

        try (var stmt = conn.prepareStatement(sqlStmts.getProperty("postgres.superuser.check"))) {
            stmt.setString(1, props.get(DbzConnectorConfig.USER));
            var res = stmt.executeQuery();
            while (res.next()) {
                isSuperUser = res.getBoolean(1);
            }
        }

        // bypass check when it is a superuser
        if (!isSuperUser) {
            // check whether user is superuser or replication role
            try (var stmt = conn.prepareStatement(sqlStmts.getProperty("postgres.role.check"))) {
                stmt.setString(1, props.get(DbzConnectorConfig.USER));
                var res = stmt.executeQuery();
                while (res.next()) {
                    if (!res.getBoolean(1)) {
                        throw new StatusException(
                                Status.INTERNAL.withDescription(
                                        "Postgres user must be superuser or replication role to start walsender."));
                    }
                }
            }
            // check whether user has select privilege on table for initial snapshot
            try (var stmt =
                    conn.prepareStatement(sqlStmts.getProperty("postgres.table_privilege.check"))) {
                stmt.setString(1, props.get(DbzConnectorConfig.TABLE_NAME));
                stmt.setString(2, props.get(DbzConnectorConfig.USER));
                var res = stmt.executeQuery();
                while (res.next()) {
                    if (!res.getBoolean(1)) {
                        throw new StatusException(
                                Status.INTERNAL.withDescription(
                                        "Postgres user must have select privilege on table "
                                                + props.get(DbzConnectorConfig.TABLE_NAME)));
                    }
                }
            }
        }

        // check whether publication exists
        boolean publicationExists = false;
        boolean partialPublication = false;
        try (var stmt = conn.createStatement()) {
            var res = stmt.executeQuery(sqlStmts.getProperty("postgres.publication_att_exists"));
            while (res.next()) {
                partialPublication = res.getBoolean(1);
            }
        }
        // pg 15 and up supports partial publication of table
        // check whether publication covers all columns
        if (partialPublication) {
            try (var stmt =
                    conn.prepareStatement(sqlStmts.getProperty("postgres.publication_att"))) {
                stmt.setString(1, props.get(DbzConnectorConfig.PG_SCHEMA_NAME));
                stmt.setString(2, props.get(DbzConnectorConfig.TABLE_NAME));
                var res = stmt.executeQuery();
                while (res.next()) {
                    String[] columnsPub = (String[]) res.getArray("attnames").getArray();
                    for (int i = 0; i < tableSchema.getColumnsCount(); i++) {
                        String columnName = tableSchema.getColumns(i).getName();
                        if (Arrays.stream(columnsPub).noneMatch(columnName::equals)) {
                            throw new StatusException(
                                    Status.INTERNAL.withDescription(
                                            "The publication 'dbz_publication' does not cover all necessary columns in table "
                                                    + props.get(DbzConnectorConfig.TABLE_NAME)));
                        }
                        if (i == tableSchema.getColumnsCount() - 1) {
                            publicationExists = true;
                        }
                    }
                    if (publicationExists) {
                        LOG.info("publication exists");
                        break;
                    }
                }
            }
        } else { // check directly whether publication exists
            try (var stmt =
                    conn.prepareStatement(sqlStmts.getProperty("postgres.publication_cnt"))) {
                stmt.setString(1, props.get(DbzConnectorConfig.PG_SCHEMA_NAME));
                stmt.setString(2, props.get(DbzConnectorConfig.TABLE_NAME));
                var res = stmt.executeQuery();
                while (res.next()) {
                    if (res.getInt("count") > 0) {
                        publicationExists = true;
                        LOG.info("publication exists");
                        break;
                    }
                }
            }
        }
        // if publication does not exist, check permission to create publication
        if (!publicationExists && !isSuperUser) {
            // check create privilege on database
            try (var stmt =
                    conn.prepareStatement(
                            sqlStmts.getProperty("postgres.database_privilege.check"))) {
                stmt.setString(1, props.get(DbzConnectorConfig.USER));
                stmt.setString(2, props.get(DbzConnectorConfig.DB_NAME));
                stmt.setString(3, props.get(DbzConnectorConfig.USER));
                var res = stmt.executeQuery();
                while (res.next()) {
                    if (!res.getBoolean(1)) {
                        throw new StatusException(
                                Status.INTERNAL.withDescription(
                                        "Postgres user must have create privilege on database"
                                                + props.get(DbzConnectorConfig.DB_NAME)));
                    }
                }
            }
            // check ownership on table
            boolean isTableOwner = false;
            String tableOwner = null;
            // check if user is owner
            try (var stmt = conn.prepareStatement(sqlStmts.getProperty("postgres.table_owner"))) {
                stmt.setString(1, props.get(DbzConnectorConfig.PG_SCHEMA_NAME));
                stmt.setString(2, props.get(DbzConnectorConfig.TABLE_NAME));
                var res = stmt.executeQuery();
                while (res.next()) {
                    tableOwner = res.getString("tableowner");
                    if (tableOwner.equals(props.get(DbzConnectorConfig.USER))) {
                        isTableOwner = true;
                        break;
                    }
                }
            }

            // if user is not owner, check if user belongs to owner group
            if (!isTableOwner && null != tableOwner) {
                try (var stmt =
                        conn.prepareStatement(sqlStmts.getProperty("postgres.users_of_group"))) {
                    stmt.setString(1, tableOwner);
                    var res = stmt.executeQuery();
                    while (res.next()) {
                        String[] users = (String[]) res.getArray("members").getArray();
                        if (Arrays.stream(users)
                                .anyMatch(props.get(DbzConnectorConfig.USER)::equals)) {
                            isTableOwner = true;
                            break;
                        }
                    }
                }
            }
            if (!isTableOwner) {
                throw new StatusException(
                        Status.INTERNAL.withDescription(
                                "Postgres user must be owner of table "
                                        + props.get(DbzConnectorConfig.TABLE_NAME)));
            }
        }
    }

    @Override
    public void close() throws Exception {
        conn.close();
    }

    private boolean isPkMatch(
            ConnectorServiceProto.TableSchema sourceSchema, Set<String> pkFields) {
        if (sourceSchema.getPkIndicesCount() != pkFields.size()) {
            return false;
        }
        for (var index : sourceSchema.getPkIndicesList()) {
            if (!pkFields.contains(sourceSchema.getColumns(index).getName())) {
                return false;
            }
        }
        return true;
    }

    private boolean isPostgresDataTypeCompatible(
            String pgDataType, Data.DataType.TypeName typeName) {
        int val = typeName.getNumber();
        switch (pgDataType) {
            case "smallint":
                return Data.DataType.TypeName.INT16_VALUE <= val
                        && val <= Data.DataType.TypeName.INT64_VALUE;
            case "integer":
                return Data.DataType.TypeName.INT32_VALUE <= val
                        && val <= Data.DataType.TypeName.INT64_VALUE;
            case "bigint":
                return val == Data.DataType.TypeName.INT64_VALUE;
            case "float":
            case "real":
                return val == Data.DataType.TypeName.FLOAT_VALUE
                        || val == Data.DataType.TypeName.DOUBLE_VALUE;
            case "boolean":
                return val == Data.DataType.TypeName.BOOLEAN_VALUE;
            case "double":
            case "double precision":
                return val == Data.DataType.TypeName.DOUBLE_VALUE;
            case "decimal":
            case "numeric":
                return val == Data.DataType.TypeName.DECIMAL_VALUE;
            case "varchar":
            case "character varying":
                return val == Data.DataType.TypeName.VARCHAR_VALUE;
            default:
                return true; // true for other uncovered types
        }
    }
}
