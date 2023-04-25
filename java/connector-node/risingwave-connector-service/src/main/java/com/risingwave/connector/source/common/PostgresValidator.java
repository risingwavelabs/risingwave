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

package com.risingwave.connector.source.common;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.proto.Data;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresValidator extends AbstractDatabaseValidator implements AutoCloseable {
    static final Logger LOG = LoggerFactory.getLogger(PostgresValidator.class);

    private final Map<String, String> userProps;

    private final TableSchema tableSchema;

    private final Connection jdbcConnection;

    public PostgresValidator(Map<String, String> userProps, TableSchema tableSchema)
            throws SQLException {
        this.userProps = userProps;
        this.tableSchema = tableSchema;

        var dbHost = userProps.get(DbzConnectorConfig.HOST);
        var dbName = userProps.get(DbzConnectorConfig.DB_NAME);
        var jdbcUrl = ValidatorUtils.getJdbcUrl(SourceTypeE.POSTGRES, dbHost, dbName);

        var user = userProps.get(DbzConnectorConfig.USER);
        var password = userProps.get(DbzConnectorConfig.PASSWORD);
        this.jdbcConnection = DriverManager.getConnection(jdbcUrl, user, password);
    }

    @Override
    public void validateDbConfig() {
        // TODO: check database server version

        // check whether source db has enabled wal
        try (var stmt = jdbcConnection.createStatement()) {
            var res = stmt.executeQuery(ValidatorUtils.getSql("postgres.wal"));
            while (res.next()) {
                if (!res.getString(1).equals("logical")) {
                    throw ValidatorUtils.invalidArgument(
                            "Postgres wal_level should be 'logical'.\nPlease modify the config and restart your Postgres server.");
                }
            }
        } catch (SQLException e) {
            throw ValidatorUtils.internalError(e);
        }
    }

    @Override
    public void validateUserPrivilege() {
        try {
            validatePrivileges();
        } catch (SQLException e) {
            throw ValidatorUtils.internalError(e);
        }
    }

    @Override
    public void validateTable() {
        try {
            validateTableSchema();
        } catch (SQLException e) {
            throw ValidatorUtils.internalError(e);
        }
    }

    /** For Citus which is a distributed version of PG */
    public void validateDistributedTable() throws SQLException {
        String schemaName = userProps.get(DbzConnectorConfig.PG_SCHEMA_NAME);
        String tableName = userProps.get(DbzConnectorConfig.TABLE_NAME);
        try (var stmt =
                jdbcConnection.prepareStatement(ValidatorUtils.getSql("citus.distributed_table"))) {
            stmt.setString(1, schemaName + "." + tableName);
            var res = stmt.executeQuery();
            while (res.next()) {
                var ret = res.getString(1);
                if (!ret.equalsIgnoreCase("distributed")) {
                    throw ValidatorUtils.invalidArgument("Citus table is not a distributed table");
                }
            }
        }
    }

    private void validateTableSchema() throws SQLException {
        String schemaName = userProps.get(DbzConnectorConfig.PG_SCHEMA_NAME);
        String tableName = userProps.get(DbzConnectorConfig.TABLE_NAME);

        try (var stmt = jdbcConnection.prepareStatement(ValidatorUtils.getSql("postgres.table"))) {
            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);
            var res = stmt.executeQuery();
            while (res.next()) {
                var ret = res.getString(1);
                if (ret.equalsIgnoreCase("f") || ret.equalsIgnoreCase("false")) {
                    throw ValidatorUtils.invalidArgument("Postgres table or schema doesn't exist");
                }
            }
        }

        // check primary key
        // reference: https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
        try (var stmt = jdbcConnection.prepareStatement(ValidatorUtils.getSql("postgres.pk"))) {
            stmt.setString(1, schemaName + "." + tableName);
            var res = stmt.executeQuery();
            var pkFields = new HashSet<String>();
            while (res.next()) {
                var name = res.getString(1);
                pkFields.add(name);
            }

            if (!ValidatorUtils.isPrimaryKeyMatch(tableSchema, pkFields)) {
                throw ValidatorUtils.invalidArgument("Primary key mismatch");
            }
        }
        // check whether source schema match table schema on upstream
        try (var stmt =
                jdbcConnection.prepareStatement(ValidatorUtils.getSql("postgres.table_schema"))) {
            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);
            var res = stmt.executeQuery();
            int index = 0;
            while (res.next()) {
                var field = res.getString(1);
                var dataType = res.getString(2);
                if (index >= tableSchema.getNumColumns()) {
                    throw ValidatorUtils.invalidArgument("The number of columns mismatch");
                }

                var srcColName = tableSchema.getColumnNames()[index++];
                if (!srcColName.equals(field)) {
                    throw ValidatorUtils.invalidArgument(
                            "table column defined in the source mismatches upstream column "
                                    + field);
                }
                if (!isDataTypeCompatible(dataType, tableSchema.getColumnType(srcColName))) {
                    throw ValidatorUtils.invalidArgument(
                            "incompatible data type of column " + srcColName);
                }
            }
        }
    }

    private void validatePrivileges() throws SQLException {
        boolean isSuperUser = false;

        try (var stmt =
                jdbcConnection.prepareStatement(
                        ValidatorUtils.getSql("postgres.superuser.check"))) {
            stmt.setString(1, userProps.get(DbzConnectorConfig.USER));
            var res = stmt.executeQuery();
            while (res.next()) {
                isSuperUser = res.getBoolean(1);
            }
        }

        // bypass check when it is a superuser
        if (!isSuperUser) {
            // check whether user is superuser or replication role
            try (var stmt =
                    jdbcConnection.prepareStatement(ValidatorUtils.getSql("postgres.role.check"))) {
                stmt.setString(1, userProps.get(DbzConnectorConfig.USER));
                var res = stmt.executeQuery();
                while (res.next()) {
                    if (!res.getBoolean(1)) {
                        throw ValidatorUtils.invalidArgument(
                                "Postgres user must be superuser or replication role to start walsender.");
                    }
                }
            }
            // check whether user has select privilege on table for initial snapshot
            try (var stmt =
                    jdbcConnection.prepareStatement(
                            ValidatorUtils.getSql("postgres.table_privilege.check"))) {
                stmt.setString(1, userProps.get(DbzConnectorConfig.TABLE_NAME));
                stmt.setString(2, userProps.get(DbzConnectorConfig.USER));
                var res = stmt.executeQuery();
                while (res.next()) {
                    if (!res.getBoolean(1)) {
                        throw ValidatorUtils.invalidArgument(
                                "Postgres user must have select privilege on table "
                                        + userProps.get(DbzConnectorConfig.TABLE_NAME));
                    }
                }
            }
        }

        // check whether publication exists
        boolean publicationExists = false;
        boolean partialPublication = false;
        try (var stmt = jdbcConnection.createStatement()) {
            var res = stmt.executeQuery(ValidatorUtils.getSql("postgres.publication_att_exists"));
            while (res.next()) {
                partialPublication = res.getBoolean(1);
            }
        }
        // pg 15 and up supports partial publication of table
        // check whether publication covers all columns
        if (partialPublication) {
            try (var stmt =
                    jdbcConnection.prepareStatement(
                            ValidatorUtils.getSql("postgres.publication_att"))) {
                stmt.setString(1, userProps.get(DbzConnectorConfig.PG_SCHEMA_NAME));
                stmt.setString(2, userProps.get(DbzConnectorConfig.TABLE_NAME));
                var res = stmt.executeQuery();
                while (res.next()) {
                    String[] columnsPub = (String[]) res.getArray("attnames").getArray();
                    List<String> attNames = Arrays.asList(columnsPub);
                    for (int i = 0; i < tableSchema.getNumColumns(); i++) {
                        String columnName = tableSchema.getColumnNames()[i];
                        if (!attNames.contains(columnName)) {
                            throw ValidatorUtils.invalidArgument(
                                    "The publication 'dbz_publication' does not cover all necessary columns in table "
                                            + userProps.get(DbzConnectorConfig.TABLE_NAME));
                        }
                        if (i == tableSchema.getNumColumns() - 1) {
                            publicationExists = true;
                        }
                    }
                    if (publicationExists) {
                        LOG.info("publication already existed");
                        break;
                    }
                }
            }
        } else { // check directly whether publication exists
            try (var stmt =
                    jdbcConnection.prepareStatement(
                            ValidatorUtils.getSql("postgres.publication_cnt"))) {
                stmt.setString(1, userProps.get(DbzConnectorConfig.PG_SCHEMA_NAME));
                stmt.setString(2, userProps.get(DbzConnectorConfig.TABLE_NAME));
                var res = stmt.executeQuery();
                while (res.next()) {
                    if (res.getInt("count") > 0) {
                        publicationExists = true;
                        LOG.info("publication already existed");
                        break;
                    }
                }
            }
        }
        // if publication does not exist, check permission to create publication
        if (!publicationExists && !isSuperUser) {
            // check create privilege on database
            try (var stmt =
                    jdbcConnection.prepareStatement(
                            ValidatorUtils.getSql("postgres.database_privilege.check"))) {
                stmt.setString(1, userProps.get(DbzConnectorConfig.USER));
                stmt.setString(2, userProps.get(DbzConnectorConfig.DB_NAME));
                stmt.setString(3, userProps.get(DbzConnectorConfig.USER));
                var res = stmt.executeQuery();
                while (res.next()) {
                    if (!res.getBoolean(1)) {
                        throw ValidatorUtils.invalidArgument(
                                "Postgres user must have create privilege on database"
                                        + userProps.get(DbzConnectorConfig.DB_NAME));
                    }
                }
            }
            // check ownership on table
            boolean isTableOwner = false;
            String tableOwner = null;
            // check if user is owner
            try (var stmt =
                    jdbcConnection.prepareStatement(
                            ValidatorUtils.getSql("postgres.table_owner"))) {
                stmt.setString(1, userProps.get(DbzConnectorConfig.PG_SCHEMA_NAME));
                stmt.setString(2, userProps.get(DbzConnectorConfig.TABLE_NAME));
                var res = stmt.executeQuery();
                while (res.next()) {
                    tableOwner = res.getString("tableowner");
                    if (tableOwner.equals(userProps.get(DbzConnectorConfig.USER))) {
                        isTableOwner = true;
                        break;
                    }
                }
            }

            // if user is not owner, check if user belongs to owner group
            if (!isTableOwner && null != tableOwner) {
                try (var stmt =
                        jdbcConnection.prepareStatement(
                                ValidatorUtils.getSql("postgres.users_of_group"))) {
                    stmt.setString(1, tableOwner);
                    var res = stmt.executeQuery();
                    while (res.next()) {
                        String[] users = (String[]) res.getArray("members").getArray();
                        if (null != users
                                && Arrays.asList(users)
                                        .contains(userProps.get(DbzConnectorConfig.USER))) {
                            isTableOwner = true;
                            break;
                        }
                    }
                }
            }
            if (!isTableOwner) {
                throw ValidatorUtils.invalidArgument(
                        "Postgres user must be owner of table "
                                + userProps.get(DbzConnectorConfig.TABLE_NAME));
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (null != jdbcConnection) {
            jdbcConnection.close();
        }
    }

    private boolean isDataTypeCompatible(String pgDataType, Data.DataType.TypeName typeName) {
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
