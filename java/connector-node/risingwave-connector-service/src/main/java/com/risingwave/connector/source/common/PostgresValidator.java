// Copyright 2025 RisingWave Labs
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
    private final boolean isCdcSourceJob;
    private final int pgVersion;

    public PostgresValidator(
            Map<String, String> userProps, TableSchema tableSchema, boolean isCdcSourceJob)
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

        this.isAwsRds =
                dbHost.contains(AWS_RDS_HOST)
                        || userProps
                                .getOrDefault(DbzConnectorConfig.PG_TEST_ONLY_FORCE_RDS, "false")
                                .equalsIgnoreCase("true");
        this.dbName = dbName;
        this.user = user;
        this.schemaName = userProps.get(DbzConnectorConfig.PG_SCHEMA_NAME);
        this.tableName = userProps.get(DbzConnectorConfig.TABLE_NAME);
        this.pubName = userProps.get(DbzConnectorConfig.PG_PUB_NAME);
        this.slotName = userProps.get(DbzConnectorConfig.PG_SLOT_NAME);

        this.pubAutoCreate =
                userProps.get(DbzConnectorConfig.PG_PUB_CREATE).equalsIgnoreCase("true");
        this.isCdcSourceJob = isCdcSourceJob;
        try {
            this.pgVersion = jdbcConnection.getMetaData().getDatabaseMajorVersion();
        } catch (SQLException e) {
            throw ValidatorUtils.internalError(e.getMessage());
        }
    }

    @Override
    public void validateDbConfig() {
        try {
            // whenever a newer PG version is released, Debezium will take
            // some time to support it. So even though 18 is not released yet, we put a version
            // guard here.
            if (pgVersion >= 18) {
                throw ValidatorUtils.failedPrecondition(
                        "Postgres major version should be less than or equal to 17.");
            }

            try (var stmt = jdbcConnection.createStatement()) {
                // check whether wal has been enabled
                var res = stmt.executeQuery(ValidatorUtils.getSql("postgres.wal"));
                while (res.next()) {
                    if (!res.getString(1).equals("logical")) {
                        throw ValidatorUtils.invalidArgument(
                                "Postgres wal_level should be 'logical'.\nPlease modify the config and restart your Postgres server.");
                    }
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
    boolean isCdcSourceJob() {
        return isCdcSourceJob;
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
        if (isCdcSourceJob) {
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
            stmt.setString(1, String.format("\"%s\".\"%s\"", this.schemaName, this.tableName));
            var res = stmt.executeQuery();
            var pkFields = new HashSet<String>();
            while (res.next()) {
                var name = res.getString(1);
                pkFields.add(name);
            }
            primaryKeyCheck(tableSchema, pkFields);
        }

        // Check whether source schema match table schema on upstream
        // All columns defined must exist in upstream database
        try (var stmt =
                jdbcConnection.prepareStatement(ValidatorUtils.getSql("postgres.table_schema"))) {
            stmt.setString(1, this.schemaName);
            stmt.setString(2, this.tableName);
            var res = stmt.executeQuery();

            // 字段名（小写） -> [数据类型, character_maximum_length]
            class ColumnInfo {
                String dataType;
                Long charMaxLength;

                ColumnInfo(String dataType, Long charMaxLength) {
                    this.dataType = dataType;
                    this.charMaxLength = charMaxLength;
                }
            }
            Map<String, ColumnInfo> schema = new HashMap<>();
            while (res.next()) {
                var field = res.getString(1);
                var dataType = res.getString(2);
                Long charMaxLength =
                        res.getObject(3) == null ? null : ((Number) res.getObject(3)).longValue();
                schema.put(field.toLowerCase(), new ColumnInfo(dataType, charMaxLength));
            }

            for (var e : tableSchema.getColumnTypes().entrySet()) {
                // skip validate internal columns
                if (e.getKey().startsWith(ValidatorUtils.INTERNAL_COLUMN_PREFIX)) {
                    continue;
                }
                var colInfo = schema.get(e.getKey().toLowerCase());
                if (colInfo == null) {
                    throw ValidatorUtils.invalidArgument(
                            "Column '" + e.getKey() + "' not found in the upstream database");
                }
                if (!isDataTypeCompatible(colInfo.dataType, e.getValue(), colInfo.charMaxLength)) {
                    throw ValidatorUtils.invalidArgument(
                            "Incompatible data type of column " + e.getKey());
                }
            }
        }
    }

    private static void primaryKeyCheck(TableSchema sourceSchema, Set<String> pkFields)
            throws RuntimeException {
        if (sourceSchema.getPrimaryKeys().size() != pkFields.size()) {
            throw ValidatorUtils.invalidArgument(
                    "Primary key mismatch: the SQL schema defines "
                            + sourceSchema.getPrimaryKeys().size()
                            + " primary key columns, but the source table in Postgres has "
                            + pkFields.size()
                            + " columns.");
        }
        for (var colName : sourceSchema.getPrimaryKeys()) {
            if (!pkFields.contains(colName)) {
                throw ValidatorUtils.invalidArgument(
                        "Primary key mismatch: The primary key list of the source table in Postgres does not contain '"
                                + colName
                                + "'.\nHint: If your primary key contains uppercase letters, please ensure that the primary key in the DML of RisingWave uses the same uppercase format and is wrapped with double quotes (\"\").");
            }
        }
    }

    private void validatePrivileges() throws SQLException {
        boolean isSuperUser = false;
        if (this.isAwsRds) {
            // check privileges for aws rds postgres
            boolean hasReplicationRole = false;
            try (var stmt =
                    jdbcConnection.prepareStatement(
                            ValidatorUtils.getSql("postgres.rds.role.check"))) {
                stmt.setString(1, this.user);
                stmt.setString(2, this.user);
                var res = stmt.executeQuery();
                while (res.next()) {
                    // check rds_superuser role or rds_replication role is granted
                    isSuperUser = res.getBoolean(1);
                    hasReplicationRole = res.getBoolean(2);
                }
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
        // cdc source job doesn't have table schema to validate, since its schema is fixed to jsonb
        if (isSuperUser || isCdcSourceJob) {
            return;
        }

        try (var stmt =
                jdbcConnection.prepareStatement(
                        ValidatorUtils.getSql("postgres.table_read_privilege.check"))) {
            stmt.setString(1, this.user);
            stmt.setString(2, String.format("\"%s\".\"%s\"", this.schemaName, this.tableName));
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

        if (!isPublicationExists && !pubAutoCreate) {
            // We require a publication on upstream to publish table cdc events
            throw ValidatorUtils.invalidArgument(
                    "Publication '" + pubName + "' doesn't exist and auto create is disabled");
        }

        // If the source properties is created by share source, skip the following
        // check of publication
        if (isCdcSourceJob) {
            if (!isPublicationExists) {
                LOG.info(
                        "creating cdc source job: publication '{}' doesn't exist, creating...",
                        pubName);
                DbzSourceUtils.createPostgresPublicationInValidate(userProps);
                LOG.info("creating cdc source job: publication '{}' created successfully", pubName);
            }
            return;
        }

        // When publication exists, we should check whether it covers the table
        try (var stmt = jdbcConnection.createStatement()) {
            var res = stmt.executeQuery(ValidatorUtils.getSql("postgres.publication_att_exists"));
            while (res.next()) {
                isPartialPublicationEnabled = res.getBoolean(1);
            }
        }

        List<String> partitions = new ArrayList<>();
        try (var stmt =
                jdbcConnection.prepareStatement(
                        ValidatorUtils.getSql("postgres.partition_names"))) {
            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);
            var res = stmt.executeQuery();
            while (res.next()) {
                partitions.add(res.getString(1));
            }
        }

        if (!partitions.isEmpty() && isPublicationExists) {
            // `pubviaroot` in `pg_publication` is added after PG v13, before which PG does not
            // allow adding partitioned table to a publication. So here, if partitions.isEmpty() is
            // false, which means the PG version is >= v13, we can safely check the value of
            // `pubviaroot` of the publication here.
            boolean isPublicationViaRoot = false;
            try (var stmt =
                    jdbcConnection.prepareStatement(
                            ValidatorUtils.getSql("postgres.publication_pubviaroot"))) {
                stmt.setString(1, pubName);
                var res = stmt.executeQuery();
                if (res.next()) {
                    isPublicationViaRoot = res.getBoolean(1);
                }
            }
            if (!isPublicationViaRoot) {
                // Make sure the publication are created with `publish_via_partition_root = true`,
                // which is required by partitioned tables.
                throw ValidatorUtils.invalidArgument(
                        "Table '"
                                + tableName
                                + "' has partitions, which requires publication '"
                                + pubName
                                + "' to be created with `publish_via_partition_root = true`. \nHint: you can run `SELECT pubviaroot from pg_publication WHERE pubname = '"
                                + pubName
                                + "'` in the upstream Postgres to check.");
            }
        }
        // Only after v13, PG allows adding a partitioned table to a publication. So, if the
        // version is before v13, the tables in a publication are always partition leaves, we don't
        // check their ancestors and descendants anymore.
        if (isPublicationExists && pgVersion >= 13) {
            List<String> family = new ArrayList<>();
            boolean findRoot = false;
            String currentPartition = tableName;
            while (!findRoot) {
                try (var stmt =
                        jdbcConnection.prepareStatement(
                                ValidatorUtils.getSql("postgres.partition_parent"))) {
                    String schemaPartitionName =
                            String.format("\"%s\".\"%s\"", this.schemaName, currentPartition);
                    stmt.setString(1, schemaPartitionName);
                    stmt.setString(2, schemaPartitionName);
                    stmt.setString(3, schemaPartitionName);
                    var res = stmt.executeQuery();
                    if (res.next()) {
                        String parent = res.getString(1);
                        family.add(parent);
                        currentPartition = parent;
                    } else {
                        findRoot = true;
                    }
                }
            }
            try (var stmt =
                    jdbcConnection.prepareStatement(
                            ValidatorUtils.getSql("postgres.partition_descendants"))) {
                String schemaTableName =
                        String.format("\"%s\".\"%s\"", this.schemaName, this.tableName);
                stmt.setString(1, schemaTableName);
                stmt.setString(2, schemaTableName);
                var res = stmt.executeQuery();
                while (res.next()) {
                    String descendant = res.getString(1);
                    family.add(descendant);
                }
            }
            // The check here was added based on experimental observations. We found that if a table
            // is added to a publication where its ancestor or descendant is already included, the
            // table cannot be read data from the slot correctly. Therefore, we must verify whether
            // its ancestors or descendants are already in the publication. If yes, we deny the
            // request.
            for (String relative : family) {
                try (var stmt =
                        jdbcConnection.prepareStatement(
                                ValidatorUtils.getSql("postgres.partition_in_publication.check"))) {
                    stmt.setString(1, schemaName);
                    stmt.setString(2, relative);
                    stmt.setString(3, pubName);
                    var res = stmt.executeQuery();
                    while (res.next()) {
                        if (res.getBoolean(1)) {
                            throw ValidatorUtils.invalidArgument(
                                    String.format(
                                            "The ancestor or descendant partition '%s' of the table partition '%s' is already covered in the publication '%s'. Please use a new publication for '%s'",
                                            relative, tableName, pubName, tableName));
                        }
                    }
                }
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
        if (isCdcSourceJob) {
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
        if (isCdcSourceJob) {
            throw ValidatorUtils.invalidArgument(
                    "The connector properties is created by a shared source unexpectedly");
        }

        String alterPublicationSql =
                String.format(
                        "ALTER PUBLICATION %s ADD TABLE %s",
                        pubName, String.format("\"%s\".\"%s\"", this.schemaName, this.tableName));
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

    private boolean isDataTypeCompatible(String pgDataType, Data.DataType.TypeName typeName, Long charMaxLength) {
        System.out.println(
                "PostgresValidator: pgDataType = "
                        + pgDataType
                        + ", typeName = "
                        + typeName
                        + ", charMaxLength = "
                        + charMaxLength);
        int val = typeName.getNumber();
        switch (pgDataType) {
            case "boolean":
                // BOOLEAN -> BOOLEAN
                return val == Data.DataType.TypeName.BOOLEAN_VALUE;
            case "bit":
                // bit类型需要结合character_maximum_length判断
                if (charMaxLength == null || charMaxLength == 1) {
                    // bit(1) 可以匹配 BOOLEAN 或 BYTEA
                    return val == Data.DataType.TypeName.BOOLEAN_VALUE
                            || val == Data.DataType.TypeName.BYTEA_VALUE;
                } else {
                    // bit(n>1) 只能匹配 BYTEA
                    return val == Data.DataType.TypeName.BYTEA_VALUE;
                }
            case "bit(1)":
                // 兼容历史写法，bit(1) 可以匹配 BOOLEAN 或 BYTEA
                return val == Data.DataType.TypeName.BOOLEAN_VALUE
                        || val == Data.DataType.TypeName.BYTEA_VALUE;
            case "smallint":
            case "smallserial":
                // SMALLINT, SMALLSERIAL -> SMALLINT
                // BOOLEAN, BIT(1) -> BOOLEAN
                return val == Data.DataType.TypeName.BOOLEAN_VALUE;
                return val == Data.DataType.TypeName.INT32_VALUE;
            case "bigint":
            case "bigserial":
            case "oid":
                // BIGINT, BIGSERIAL, OID -> BIGINT
                return val == Data.DataType.TypeName.INT64_VALUE;
            case "real":
                // REAL -> REAL
                return val == Data.DataType.TypeName.FLOAT_VALUE;
            case "double":
            case "double precision":
                // DOUBLE PRECISION -> DOUBLE PRECISION
                return val == Data.DataType.TypeName.DOUBLE_VALUE;
            case "char":
            case "character":
            case "varchar":
            case "character varying":
            case "xml":
            case "uuid":
            case "text":
            case "inet":
            case "cidr":
            case "macaddr":
            case "macaddr8":
            case "int4range":
            case "int8range":
            case "numrange":
            case "tsrange":
            case "tstzrange":
            case "daterange":
            case "enum":
                // CHARACTER, CHARACTER VARYING, VARCHAR, CHAR, XML, UUID, CITEXT, INET, CIDR,
                // MACADDR, MACADDR8, INT4RANGE, INT8RANGE, NUMRANGE, TSRANGE, TSTZRANGE, DATERANGE,
                // ENUM -> CHARACTER VARYING
                return val == Data.DataType.TypeName.VARCHAR_VALUE;
            case "timestamptz":
            case "timestamp with time zone":
                // TIMESTAMPTZ, TIMESTAMP WITH TIME ZONE -> TIMESTAMP WITH TIME ZONE
                return val == Data.DataType.TypeName.TIMESTAMPTZ_VALUE;
            case "timetz":
            case "time with time zone":
            case "time without time zone":
                // TIMETZ, TIME WITH TIME ZONE -> TIME WITHOUT TIME ZONE (assume UTC)
                return val == Data.DataType.TypeName.TIME_VALUE;
            case "interval":
                // INTERVAL [P] -> INTERVAL
                return val == Data.DataType.TypeName.INTERVAL_VALUE;
            case "bytea":
                // BYTEA -> BYTEA
                return val == Data.DataType.TypeName.BYTEA_VALUE;
            case "json":
            case "jsonb":
                // JSON, JSONB -> JSONB
                return val == Data.DataType.TypeName.JSONB_VALUE;
            case "date":
                // DATE -> DATE
                return val == Data.DataType.TypeName.DATE_VALUE;
            case "time":
            case "time(1)":
            case "time(2)":
            case "time(3)":
            case "time(4)":
            case "time(5)":
            case "time(6)":
                // TIME(N) -> TIME WITHOUT TIME ZONE
                return val == Data.DataType.TypeName.TIME_VALUE;
            case "timestamp":
            case "timestamp without time zone":
            case "timestamp(1)":
            case "timestamp(2)":
            case "timestamp(3)":
            case "timestamp(4)":
            case "timestamp(5)":
            case "timestamp(6)":
                // TIMESTAMP(N) -> TIMESTAMP WITHOUT TIME ZONE
                return val == Data.DataType.TypeName.TIMESTAMP_VALUE;
            case "numeric":
            case "decimal":
                // NUMERIC, DECIMAL -> DECIMAL, INT256, VARCHAR
                return val == Data.DataType.TypeName.DECIMAL_VALUE
                        || val == Data.DataType.TypeName.INT256_VALUE
                        || val == Data.DataType.TypeName.VARCHAR_VALUE;
            case "money":
                // MONEY -> NUMERIC
                return val == Data.DataType.TypeName.DECIMAL_VALUE;
            case "point":
                // POINT -> STRUCT (not supported, return false)
                return false;
            default:
                return false; // false for other uncovered types
        }
    }
}
