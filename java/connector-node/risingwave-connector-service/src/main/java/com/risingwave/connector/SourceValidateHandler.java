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

import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.Data;
import com.risingwave.sourcenode.common.DbzConnectorConfig;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceValidateHandler {
    static final Logger LOG = LoggerFactory.getLogger(SourceValidateHandler.class);
    private final StreamObserver<ConnectorServiceProto.ValidateSourceResponse> responseObserver;

    public SourceValidateHandler(
            StreamObserver<ConnectorServiceProto.ValidateSourceResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    public void handle(ConnectorServiceProto.ValidateSourceRequest request) {
        validateDbProperties(request, this.responseObserver);
    }

    private void validateDbProperties(
            ConnectorServiceProto.ValidateSourceRequest request,
            StreamObserver<ConnectorServiceProto.ValidateSourceResponse> responseObserver) {
        var props = request.getPropertiesMap();
        String jdbcUrl =
                toJdbcPrefix(request.getSourceType())
                        + "://"
                        + props.get(DbzConnectorConfig.HOST)
                        + ":"
                        + props.get(DbzConnectorConfig.PORT)
                        + "/"
                        + props.get(DbzConnectorConfig.DB_NAME);
        LOG.debug("validate jdbc url: {}", jdbcUrl);

        var sqlStmts = new Properties();
        try (var input =
                getClass().getClassLoader().getResourceAsStream("validate_sql.properties")) {
            sqlStmts.load(input);
        } catch (IOException e) {
            LOG.error("failed to load sql statements", e);
            throw new RuntimeException(e);
        }
        try (var conn =
                DriverManager.getConnection(
                        jdbcUrl,
                        props.get(DbzConnectorConfig.USER),
                        props.get(DbzConnectorConfig.PASSWORD))) {
            // usernamed and password are correct
            var dbMeta = conn.getMetaData();

            LOG.debug("source schema: {}", request.getTableSchema().getColumnsList());
            LOG.debug("source pk: {}", request.getTableSchema().getPkIndicesList());

            // validate schema name and table name
            switch (request.getSourceType()) {
                case MYSQL:
                    // check whether source db has enabled binlog
                    try (var stmt = conn.createStatement()) {
                        var res = stmt.executeQuery(sqlStmts.getProperty("mysql.bin_log"));
                        while (res.next()) {
                            if (!res.getString(2).equalsIgnoreCase("ON")) {
                                throw new StatusException(
                                        Status.INTERNAL.withDescription(
                                                "MySQL doesn't enable binlog.\nPlease set the value of log_bin to 'ON' and restart your MySQL server."));
                            }
                        }
                    }
                    // check binlog format
                    try (var stmt = conn.createStatement()) {
                        var res = stmt.executeQuery(sqlStmts.getProperty("mysql.bin_format"));
                        while (res.next()) {
                            if (!res.getString(2).equalsIgnoreCase("ROW")) {
                                throw new StatusException(
                                        Status.INTERNAL.withDescription(
                                                "MySQL binlog_format should be 'ROW'.\nPlease modify the config and restart your MySQL server."));
                            }
                        }
                    }
                    try (var stmt = conn.createStatement()) {
                        var res = stmt.executeQuery(sqlStmts.getProperty("mysql.bin_row_image"));
                        while (res.next()) {
                            if (!res.getString(2).equalsIgnoreCase("FULL")) {
                                throw new StatusException(
                                        Status.INTERNAL.withDescription(
                                                "MySQL binlog_row_image should be 'FULL'.\nPlease modify the config and restart your MySQL server."));
                            }
                        }
                    }
                    // check whether table exist
                    try (var stmt = conn.prepareStatement(sqlStmts.getProperty("mysql.table"))) {
                        stmt.setString(1, props.get(DbzConnectorConfig.DB_NAME));
                        stmt.setString(2, props.get(DbzConnectorConfig.TABLE_NAME));
                        var res = stmt.executeQuery();
                        while (res.next()) {
                            var ret = res.getInt(1);
                            if (ret == 0) {
                                throw new RuntimeException("MySQL table doesn't exist");
                            }
                        }
                    }
                    // check whether PK constraint match source table definition
                    try (var stmt =
                            conn.prepareStatement(sqlStmts.getProperty("mysql.table_schema"))) {
                        var sourceSchema = request.getTableSchema();
                        stmt.setString(1, props.get(DbzConnectorConfig.DB_NAME));
                        stmt.setString(2, props.get(DbzConnectorConfig.TABLE_NAME));
                        var res = stmt.executeQuery();
                        var pkFields = new HashSet<String>();
                        int index = 0;
                        while (res.next()) {
                            var field = res.getString(1);
                            var dataType = res.getString(2);
                            var key = res.getString(3);

                            if (index >= sourceSchema.getColumnsCount()) {
                                throw new RuntimeException(("The number of columns mismatch"));
                            }

                            var srcCol = sourceSchema.getColumns(index++);
                            if (!srcCol.getName().equals(field)) {
                                throw new RuntimeException(
                                        String.format(
                                                "column name mismatch: %s, [%s]",
                                                field, srcCol.getName()));
                            }
                            if (!isMySQLDataTypeCompatible(dataType, srcCol.getDataType())) {
                                throw new RuntimeException(
                                        String.format(
                                                "incompatible data type of column %s",
                                                srcCol.getName()));
                            }
                            if (key.equalsIgnoreCase("PRI")) {
                                pkFields.add(field);
                            }
                        }

                        if (!isPkMatch(sourceSchema, pkFields)) {
                            throw new RuntimeException("Primary key mismatch");
                        }
                    }
                    break;
                case POSTGRES:
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
                    // check schema name and table name
                    try (var stmt = conn.prepareStatement(sqlStmts.getProperty("postgres.table"))) {
                        stmt.setString(1, props.get(DbzConnectorConfig.PG_SCHEMA_NAME));
                        stmt.setString(2, props.get(DbzConnectorConfig.TABLE_NAME));
                        var res = stmt.executeQuery();
                        while (res.next()) {
                            var ret = res.getString(1);
                            if (ret.equalsIgnoreCase("f") || ret.equalsIgnoreCase("false")) {
                                throw new RuntimeException(
                                        "Postgres table or schema doesn't exist");
                            }
                        }
                    }
                    // check primary key
                    // reference: https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
                    try (var stmt = conn.prepareStatement(sqlStmts.getProperty("postgres.pk"))) {
                        stmt.setString(
                                1,
                                props.get(DbzConnectorConfig.PG_SCHEMA_NAME)
                                        + "."
                                        + props.get(DbzConnectorConfig.TABLE_NAME));

                        var res = stmt.executeQuery();
                        var pkFields = new HashSet<String>();
                        while (res.next()) {
                            var name = res.getString(1);
                            pkFields.add(name);
                        }

                        if (!isPkMatch(request.getTableSchema(), pkFields)) {
                            throw new RuntimeException("Primary key mismatch");
                        }
                    }
                    // check whether source schema match table schema on upstream
                    try (var stmt =
                            conn.prepareStatement(sqlStmts.getProperty("postgres.table_schema"))) {
                        stmt.setString(1, props.get(DbzConnectorConfig.PG_SCHEMA_NAME));
                        stmt.setString(2, props.get(DbzConnectorConfig.TABLE_NAME));
                        var res = stmt.executeQuery();
                        var sourceSchema = request.getTableSchema();
                        int index = 0;
                        while (res.next()) {
                            var field = res.getString(1);
                            var dataType = res.getString(2);
                            if (index >= sourceSchema.getColumnsCount()) {
                                throw new RuntimeException("The number of columns mismatch");
                            }
                            var srcCol = sourceSchema.getColumns(index++);
                            if (!srcCol.getName().equals(field)) {
                                throw new RuntimeException(
                                        String.format(
                                                "table column defined in the source mismatches upstream column %s",
                                                field));
                            }
                            if (!isPostgresDataTypeCompatible(dataType, srcCol.getDataType())) {
                                throw new RuntimeException(
                                        String.format(
                                                "incompatible data type of column %s",
                                                srcCol.getName()));
                            }
                        }
                    }
                    // check whether user is superuser or replication role
                    try (var stmt =
                            conn.prepareStatement(sqlStmts.getProperty("postgres.role.check"))) {
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
                            conn.prepareStatement(
                                    sqlStmts.getProperty("postgres.table_privilege.check"))) {
                        stmt.setString(1, props.get(DbzConnectorConfig.TABLE_NAME));
                        stmt.setString(2, props.get(DbzConnectorConfig.USER));
                        var res = stmt.executeQuery();
                        while (res.next()) {
                            if (!res.getBoolean(1)) {
                                throw new StatusException(
                                        Status.INTERNAL.withDescription(
                                                "Postgres user must have select privilege on table "
                                                        + props.get(
                                                                DbzConnectorConfig.TABLE_NAME)));
                            }
                        }
                    }
                    // check whether publication exists
                    boolean publicationExists = false;
                    boolean partialPublication = false;
                    try (var stmt = conn.createStatement()) {
                        var res =
                                stmt.executeQuery(
                                        sqlStmts.getProperty("postgres.publication_att_exists"));
                        while (res.next()) {
                            partialPublication = res.getBoolean(1);
                        }
                    }
                    // pg 15 and up supports partial publication of table
                    // check whether publication covers all columns
                    if (partialPublication) {
                        try (var stmt =
                                conn.prepareStatement(
                                        sqlStmts.getProperty("postgres.publication_att"))) {
                            stmt.setString(1, props.get(DbzConnectorConfig.PG_SCHEMA_NAME));
                            stmt.setString(2, props.get(DbzConnectorConfig.TABLE_NAME));
                            var res = stmt.executeQuery();
                            while (res.next()) {
                                String[] columnsPub =
                                        (String[]) res.getArray("attnames").getArray();
                                var sourceSchema = request.getTableSchema();
                                for (int i = 0; i < sourceSchema.getColumnsCount(); i++) {
                                    String columnName = sourceSchema.getColumns(i).getName();
                                    if (Arrays.stream(columnsPub).noneMatch(columnName::equals)) {
                                        throw new StatusException(
                                                Status.INTERNAL.withDescription(
                                                        "The publication 'dbz_publication' does not cover all necessary columns in table "
                                                                + props.get(
                                                                        DbzConnectorConfig
                                                                                .TABLE_NAME)));
                                    }
                                    if (i == sourceSchema.getColumnsCount() - 1) {
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
                                conn.prepareStatement(
                                        sqlStmts.getProperty("postgres.publication_cnt"))) {
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
                    if (!publicationExists) {
                        // check create privilege on database
                        try (var stmt =
                                conn.prepareStatement(
                                        sqlStmts.getProperty(
                                                "postgres.database_privilege.check"))) {
                            stmt.setString(1, props.get(DbzConnectorConfig.USER));
                            stmt.setString(2, props.get(DbzConnectorConfig.DB_NAME));
                            stmt.setString(3, props.get(DbzConnectorConfig.USER));
                            var res = stmt.executeQuery();
                            while (res.next()) {
                                if (!res.getBoolean(1)) {
                                    throw new StatusException(
                                            Status.INTERNAL.withDescription(
                                                    "Postgres user must have create privilege on database"
                                                            + props.get(
                                                                    DbzConnectorConfig.DB_NAME)));
                                }
                            }
                        }
                        // check ownership on table
                        boolean isTableOwner = false;
                        String owner = null;
                        // check if user is owner
                        try (var stmt =
                                conn.prepareStatement(
                                        sqlStmts.getProperty("postgres.table_owner"))) {
                            stmt.setString(1, props.get(DbzConnectorConfig.PG_SCHEMA_NAME));
                            stmt.setString(2, props.get(DbzConnectorConfig.TABLE_NAME));
                            var res = stmt.executeQuery();
                            while (res.next()) {
                                owner = res.getString("tableowner");
                                if (owner.equals(props.get(DbzConnectorConfig.USER))) {
                                    isTableOwner = true;
                                    break;
                                }
                            }
                        }
                        // if user is not owner, check if user belongs to owner group
                        if (!isTableOwner && !owner.isEmpty()) {
                            try (var stmt =
                                    conn.prepareStatement(
                                            sqlStmts.getProperty("postgres.users_of_group"))) {
                                stmt.setString(1, owner);
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
                    break;
                default:
                    break;
            }

            LOG.info(
                    "validate properties success. product: {}, version: {}",
                    dbMeta.getDatabaseProductName(),
                    dbMeta.getDatabaseProductVersion());
            responseObserver.onCompleted();

        } catch (Exception e) {
            LOG.error("invalid connector properties:", e);
            responseObserver.onError(
                    new StatusException(Status.INVALID_ARGUMENT.withDescription(e.getMessage())));
        }
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

    private boolean isMySQLDataTypeCompatible(
            String mysqlDataType, Data.DataType.TypeName typeName) {
        int val = typeName.getNumber();
        switch (mysqlDataType) {
            case "tinyint": // boolean
                return (val == Data.DataType.TypeName.BOOLEAN_VALUE)
                        || (Data.DataType.TypeName.INT16_VALUE <= val
                                && val <= Data.DataType.TypeName.INT64_VALUE);
            case "smallint":
                return Data.DataType.TypeName.INT16_VALUE <= val
                        && val <= Data.DataType.TypeName.INT64_VALUE;
            case "mediumint":
            case "int":
                return Data.DataType.TypeName.INT32_VALUE <= val
                        && val <= Data.DataType.TypeName.INT64_VALUE;
            case "bigint":
                return val == Data.DataType.TypeName.INT64_VALUE;

            case "float":
            case "real":
                return val == Data.DataType.TypeName.FLOAT_VALUE
                        || val == Data.DataType.TypeName.DOUBLE_VALUE;
            case "double":
                return val == Data.DataType.TypeName.DOUBLE_VALUE;
            case "decimal":
                return val == Data.DataType.TypeName.DECIMAL_VALUE;
            case "varchar":
                return val == Data.DataType.TypeName.VARCHAR_VALUE;
            default:
                return true; // true for other uncovered types
        }
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

    private String toJdbcPrefix(ConnectorServiceProto.SourceType sourceType) {
        switch (sourceType) {
            case MYSQL:
                return "jdbc:mysql";
            case POSTGRES:
                return "jdbc:postgresql";
            default:
                return "";
        }
    }
}
