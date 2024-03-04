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

public class PostgresValidator extends DatabaseValidator implements AutoCloseable {
    static final Logger LOG = LoggerFactory.getLogger(PostgresValidator.class);

    private final Map<String, String> userProps;

    private final TableSchema tableSchema;

    private final Connection jdbcConnection;

    private final String user;
    private final String dbName;
    private final String schemaName;
    private final String tableName;
    private final String pubName;
    private final String slotName;

    private final boolean pubAutoCreate;

    private static final String AWS_RDS_HOST = "amazonaws.com";
    private final boolean isAwsRds;

    // Whether the properties to validate is shared by multiple tables.
    // If true, we will skip validation check for table
    private final boolean isMultiTableShared;

    public PostgresValidator(
            Map<String, String> userProps, TableSchema tableSchema, boolean isMultiTableShared)
            throws SQLException {
        this.userProps = userProps;
        this.tableSchema = tableSchema;

        var dbHost = userProps.get(DbzConnectorConfig.HOST);
        var dbPort = userProps.get(DbzConnectorConfig.PORT);
        var dbName = userProps.get(DbzConnectorConfig.DB_NAME);
        var jdbcUrl = ValidatorUtils.getJdbcUrl(SourceTypeE.POSTGRES, dbHost, dbPort, dbName);

        var user = userProps.get(DbzConnectorConfig.USER);
        var password = userProps.get(DbzConnectorConfig.PASSWORD);
        this.jdbcConnection = DriverManager.getConnection(jdbcUrl, user, password);

        this.isAwsRds = dbHost.contains(AWS_RDS_HOST);
        this.dbName = dbName;
        this.user = user;
        this.schemaName = userProps.get(DbzConnectorConfig.PG_SCHEMA_NAME);
        this.tableName = userProps.get(DbzConnectorConfig.TABLE_NAME);
        this.pubName = userProps.get(DbzConnectorConfig.PG_PUB_NAME);
        this.slotName = userProps.get(DbzConnectorConfig.PG_SLOT_NAME);

        this.pubAutoCreate =
                userProps.get(DbzConnectorConfig.PG_PUB_CREATE).equalsIgnoreCase("true");
        this.isMultiTableShared = isMultiTableShared;
    }

    @Override
    public void validateDbConfig() {
        // TODO: check database server version
        try (var stmt = jdbcConnection.createStatement()) {
            // check whether wal has been enabled
            var res = stmt.executeQuery(ValidatorUtils.getSql("postgres.wal"));
            while (res.next()) {
                if (!res.getString(1).equals("logical")) {
                    throw ValidatorUtils.invalidArgument(
                            "Postgres wal_level should be 'logical'.\nPlease modify the config and restart your Postgres server.");
                }
            }
        } catch (SQLException e) {
            throw ValidatorUtils.internalError(e.getMessage());
        }

        try (var stmt =
                jdbcConnection.prepareStatement(ValidatorUtils.getSql("postgres.slot.check"))) {
            // check whether the replication slot is already existed
            stmt.setString(1, slotName);
            stmt.setString(2, dbName);
            var res = stmt.executeQuery();
            if (res.next() && res.getString(1).equals(slotName)) {
                LOG.info("replication slot '{}' already exists, just use it", slotName);
            } else {
                // otherwise, we need to create a new one
                var stmt2 =
                        jdbcConnection.prepareStatement(
                                ValidatorUtils.getSql("postgres.slot_limit.check"));
                var res2 = stmt2.executeQuery();
                // check whether the number of replication slots reaches the max limit
                if (res2.next() && res2.getString(1).equals("true")) {
                    throw ValidatorUtils.failedPrecondition(
                            "all replication slots are in use\n Hint: Free one or increase max_replication_slots.");
                }
            }
        } catch (SQLException e) {
            throw ValidatorUtils.internalError(e.getMessage());
        }
    }

    @Override
    public void validateUserPrivilege() {
        try {
            validatePrivileges();
        } catch (SQLException e) {
            throw ValidatorUtils.internalError(e.getMessage());
        }
    }

    @Override
    public void validateTable() {
        try {
            validateTableSchema();
        } catch (SQLException e) {
            throw ValidatorUtils.internalError(e.getMessage());
        }
    }

    @Override
    boolean isMultiTableShared() {
        return isMultiTableShared;
    }

    /** For Citus which is a distributed version of PG */
    public void validateDistributedTable() throws SQLException {
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
        if (isMultiTableShared) {
            return;
        }
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
            stmt.setString(1, this.schemaName + "." + this.tableName);
            var res = stmt.executeQuery();
            var pkFields = new HashSet<String>();
            while (res.next()) {
                var name = res.getString(1);
                // RisingWave always use lower case for column name
                pkFields.add(name.toLowerCase());
            }

            if (!ValidatorUtils.isPrimaryKeyMatch(tableSchema, pkFields)) {
                throw ValidatorUtils.invalidArgument("Primary key mismatch");
            }
        }

        // Check whether source schema match table schema on upstream
        // All columns defined must exist in upstream database
        try (var stmt =
                jdbcConnection.prepareStatement(ValidatorUtils.getSql("postgres.table_schema"))) {
            stmt.setString(1, this.schemaName);
            stmt.setString(2, this.tableName);
            var res = stmt.executeQuery();

            // Field names in lower case -> data type
            Map<String, String> schema = new HashMap<>();
            while (res.next()) {
                var field = res.getString(1);
                var dataType = res.getString(2);
                schema.put(field.toLowerCase(), dataType);
            }

            for (var e : tableSchema.getColumnTypes().entrySet()) {
                // skip validate internal columns
                if (e.getKey().startsWith(ValidatorUtils.INTERNAL_COLUMN_PREFIX)) {
                    continue;
                }
                var dataType = schema.get(e.getKey().toLowerCase());
                if (dataType == null) {
                    throw ValidatorUtils.invalidArgument(
                            "Column '" + e.getKey() + "' not found in the upstream database");
                }
                if (!isDataTypeCompatible(dataType, e.getValue())) {
                    throw ValidatorUtils.invalidArgument(
                            "Incompatible data type of column " + e.getKey());
                }
            }
        }
    }

    private void validatePrivileges() throws SQLException {
        boolean isSuperUser = false;
        if (this.isAwsRds) {
            // check privileges for aws rds postgres
            boolean hasReplicationRole;
            try (var stmt =
                    jdbcConnection.prepareStatement(
                            ValidatorUtils.getSql("postgres.rds.role.check"))) {
                stmt.setString(1, this.user);
                var res = stmt.executeQuery();
                var hashSet = new HashSet<String>();
                while (res.next()) {
                    // check rds_superuser role or rds_replication role is granted
                    var memberof = res.getArray("memberof");
                    if (memberof != null) {
                        var members = (String[]) memberof.getArray();
                        hashSet.addAll(Arrays.asList(members));
                    }
                    LOG.info("rds memberof: {}", hashSet);
                }
                isSuperUser = hashSet.contains("rds_superuser");
                hasReplicationRole = hashSet.contains("rds_replication");
            }

            if (!isSuperUser && !hasReplicationRole) {
                throw ValidatorUtils.invalidArgument(
                        "Postgres user must be superuser or replication role to start walsender.");
            }
        } else {
            // check privileges for standalone postgres
            try (var stmt =
                    jdbcConnection.prepareStatement(
                            ValidatorUtils.getSql("postgres.superuser.check"))) {
                stmt.setString(1, this.user);
                var res = stmt.executeQuery();
                while (res.next()) {
                    isSuperUser = res.getBoolean(1);
                }
            }
            if (!isSuperUser) {
                // check whether user has replication role
                try (var stmt =
                        jdbcConnection.prepareStatement(
                                ValidatorUtils.getSql("postgres.role.check"))) {
                    stmt.setString(1, this.user);
                    var res = stmt.executeQuery();
                    while (res.next()) {
                        if (!res.getBoolean(1)) {
                            throw ValidatorUtils.invalidArgument(
                                    "Postgres user must be superuser or replication role to start walsender.");
                        }
                    }
                }
            }
        }

        // check whether select privilege on table for snapshot read
        validateTablePrivileges(isSuperUser);
        validatePublicationConfig(isSuperUser);
    }

    private void validateTablePrivileges(boolean isSuperUser) throws SQLException {
        if (isSuperUser || isMultiTableShared) {
            return;
        }

        try (var stmt =
                jdbcConnection.prepareStatement(
                        ValidatorUtils.getSql("postgres.table_read_privilege.check"))) {
            stmt.setString(1, this.schemaName);
            stmt.setString(2, this.tableName);
            stmt.setString(3, this.user);
            var res = stmt.executeQuery();
            while (res.next()) {
                if (!res.getBoolean(1)) {
                    throw ValidatorUtils.invalidArgument(
                            "Postgres user must have select privilege on table '"
                                    + schemaName
                                    + "."
                                    + tableName
                                    + "'");
                }
            }
        }
    }

    /* Check required privilege to create/alter a publication */
    private void validatePublicationConfig(boolean isSuperUser) throws SQLException {
        boolean isPublicationCoversTable = false;
        boolean isPublicationExists = false;
        boolean isPartialPublicationEnabled = false;

        // Check whether publication exists
        try (var stmt =
                jdbcConnection.prepareStatement(
                        ValidatorUtils.getSql("postgres.publication_exist"))) {
            stmt.setString(1, pubName);
            var res = stmt.executeQuery();
            while (res.next()) {
                isPublicationExists = res.getBoolean(1);
            }
        }

        if (!isPublicationExists) {
            // We require a publication on upstream to publish table cdc events
            if (!pubAutoCreate) {
                throw ValidatorUtils.invalidArgument(
                        "Publication '" + pubName + "' doesn't exist and auto create is disabled");
            } else {
                // createPublicationIfNeeded(Optional.empty());
                LOG.info(
                        "Publication '{}' doesn't exist, will be created in the process of streaming job.",
                        this.pubName);
            }
        }

        // If the source properties is shared by multiple tables, skip the following
        // check of publication
        if (isMultiTableShared) {
            return;
        }

        // When publication exists, we should check whether it covers the table
        try (var stmt = jdbcConnection.createStatement()) {
            var res = stmt.executeQuery(ValidatorUtils.getSql("postgres.publication_att_exists"));
            while (res.next()) {
                isPartialPublicationEnabled = res.getBoolean(1);
            }
        }
        // PG 15 and up supports partial publication of table
        // check whether publication covers all columns of the table schema
        if (isPartialPublicationEnabled) {
            try (var stmt =
                    jdbcConnection.prepareStatement(
                            ValidatorUtils.getSql("postgres.publication_attnames"))) {
                stmt.setString(1, schemaName);
                stmt.setString(2, tableName);
                stmt.setString(3, pubName);
                var res = stmt.executeQuery();
                while (res.next()) {
                    String[] columnsPub = (String[]) res.getArray("attnames").getArray();
                    List<String> attNames = Arrays.asList(columnsPub);
                    for (int i = 0; i < tableSchema.getNumColumns(); i++) {
                        String columnName = tableSchema.getColumnNames()[i];
                        if (!attNames.contains(columnName)) {
                            throw ValidatorUtils.invalidArgument(
                                    String.format(
                                            "The publication '%s' does not cover all columns of the table '%s'",
                                            pubName, schemaName + "." + tableName));
                        }
                        if (i == tableSchema.getNumColumns() - 1) {
                            isPublicationCoversTable = true;
                        }
                    }
                    if (isPublicationCoversTable) {
                        LOG.info(
                                "The publication covers the table '{}'.",
                                schemaName + "." + tableName);
                        break;
                    }
                }
            }
        } else {
            // PG <= 14.0
            // check whether the publication covers the subscribed table
            try (var stmt =
                    jdbcConnection.prepareStatement(
                            ValidatorUtils.getSql("postgres.publication_has_table"))) {
                stmt.setString(1, schemaName);
                stmt.setString(2, tableName);
                stmt.setString(3, pubName);
                var res = stmt.executeQuery();
                if (res.next()) {
                    isPublicationCoversTable = res.getBoolean(1);
                    if (isPublicationCoversTable) {
                        LOG.info(
                                "The publication covers the table '{}'.",
                                schemaName + "." + tableName);
                    }
                }
            }
        }

        // If auto create is enabled and the publication doesn't exist or doesn't cover the table,
        // we need to create or alter the publication. And we need to check the required privileges.
        if (!isPublicationCoversTable) {
            // check whether the user has the CREATE privilege on database
            if (!isSuperUser) {
                validatePublicationPrivileges();
            }
            if (isPublicationExists) {
                alterPublicationIfNeeded();
            } else {
                LOG.info(
                        "Publication '{}' doesn't exist, will be created in the process of streaming job.",
                        this.pubName);
            }
        }
    }

    private void validatePublicationPrivileges() throws SQLException {
        if (isMultiTableShared) {
            throw ValidatorUtils.invalidArgument(
                    "The connector properties is shared by multiple tables unexpectedly");
        }

        // check whether the user has the CREATE privilege on database
        try (var stmt =
                jdbcConnection.prepareStatement(
                        ValidatorUtils.getSql("postgres.database_privilege.check"))) {
            stmt.setString(1, this.user);
            stmt.setString(2, this.dbName);
            stmt.setString(3, this.user);
            var res = stmt.executeQuery();
            while (res.next()) {
                if (!res.getBoolean(1)) {
                    throw ValidatorUtils.invalidArgument(
                            "Postgres user must have create privilege on database '"
                                    + this.dbName
                                    + "'");
                }
            }
        }

        // check whether the user has ownership on the table
        boolean isTableOwner = false;
        String tableOwner = null;
        // check if user is the direct owner of table
        try (var stmt =
                jdbcConnection.prepareStatement(ValidatorUtils.getSql("postgres.table_owner"))) {
            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);
            var res = stmt.executeQuery();
            while (res.next()) {
                tableOwner = res.getString("tableowner");
                if (tableOwner != null && tableOwner.equals(this.user)) {
                    isTableOwner = true;
                    break;
                }
            }
        }

        // if user is not the direct owner, check if user belongs to an owner group
        if (!isTableOwner && null != tableOwner) {
            try (var stmt =
                    jdbcConnection.prepareStatement(
                            ValidatorUtils.getSql("postgres.users_of_group"))) {
                stmt.setString(1, tableOwner);
                var res = stmt.executeQuery();
                while (res.next()) {
                    var usersArray = res.getArray("members");
                    if (usersArray == null) {
                        break;
                    }
                    String[] users = (String[]) usersArray.getArray();
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
                    "Postgres user must be the owner of table '"
                            + tableName
                            + "' to create/alter publication");
        }
    }

    protected void alterPublicationIfNeeded() throws SQLException {
        if (isMultiTableShared) {
            throw ValidatorUtils.invalidArgument(
                    "The connector properties is shared by multiple tables unexpectedly");
        }

        String alterPublicationSql =
                String.format(
                        "ALTER PUBLICATION %s ADD TABLE %s", pubName, schemaName + "." + tableName);
        try (var stmt = jdbcConnection.createStatement()) {
            LOG.info("Altered publication with statement: {}", alterPublicationSql);
            stmt.execute(alterPublicationSql);
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
