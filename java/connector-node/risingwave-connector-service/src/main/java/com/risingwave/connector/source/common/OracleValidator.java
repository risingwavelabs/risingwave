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

import com.risingwave.connector.api.source.SourceTypeE;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class OracleValidator extends DatabaseValidator implements AutoCloseable {
    private static final Pattern ORACLE_UNQUOTED_IDENTIFIER =
            Pattern.compile("[A-Za-z][A-Za-z0-9_$#]*");

    private static final Set<String> REQUIRED_PRIVILEGES =
            Set.of(
                    "CREATE SESSION",
                    "SET CONTAINER",
                    "FLASHBACK ANY TABLE",
                    "SELECT ANY TABLE",
                    "SELECT ANY TRANSACTION",
                    "LOGMINING",
                    "LOCK ANY TABLE",
                    "CREATE TABLE",
                    "CREATE SEQUENCE");

    private static final Set<String> REQUIRED_ROLES =
            Set.of("SELECT_CATALOG_ROLE", "EXECUTE_CATALOG_ROLE");

    private final Connection jdbcConnection;
    private final String pdbName;
    private final String schemaName;
    private final String tableName;
    private final boolean isCdcSourceJob;

    public OracleValidator(Map<String, String> userProps, boolean isCdcSourceJob)
            throws SQLException {
        var jdbcUrl =
                ValidatorUtils.getJdbcUrl(
                        SourceTypeE.ORACLE,
                        userProps.get(DbzConnectorConfig.HOST),
                        userProps.get(DbzConnectorConfig.PORT),
                        userProps.get(DbzConnectorConfig.DB_NAME));
        this.jdbcConnection =
                DriverManager.getConnection(
                        jdbcUrl,
                        userProps.get(DbzConnectorConfig.USER),
                        userProps.get(DbzConnectorConfig.PASSWORD));
        this.pdbName = normalizePdbName(userProps.get(DbzConnectorConfig.ORACLE_PDB_NAME));
        this.schemaName = userProps.get(DbzConnectorConfig.ORACLE_SCHEMA_NAME);
        this.tableName = userProps.get(DbzConnectorConfig.TABLE_NAME);
        this.isCdcSourceJob = isCdcSourceJob;
    }

    @Override
    void validateDbConfig() {
        try {
            validateLoggingConfiguration();
            validatePdb();
        } catch (SQLException e) {
            throw ValidatorUtils.internalError(e.getMessage());
        }
    }

    private void validateLoggingConfiguration() throws SQLException {
        try (var stmt = jdbcConnection.createStatement();
                var result =
                        stmt.executeQuery(
                                "SELECT LOG_MODE, FORCE_LOGGING, SUPPLEMENTAL_LOG_DATA_MIN "
                                        + "FROM V$DATABASE")) {
            if (!result.next()) {
                throw ValidatorUtils.invalidArgument(
                        "Unable to read Oracle logging configuration from V$DATABASE");
            }
            validateLoggingConfiguration(
                    result.getString("LOG_MODE"),
                    result.getString("FORCE_LOGGING"),
                    result.getString("SUPPLEMENTAL_LOG_DATA_MIN"));
        }
    }

    static void validateLoggingConfiguration(
            String logMode, String forceLogging, String supplementalLogDataMin) {
        if (!"ARCHIVELOG".equalsIgnoreCase(logMode)) {
            throw ValidatorUtils.failedPrecondition(
                    "Oracle must run in ARCHIVELOG mode for LogMiner CDC");
        }
        if (!"YES".equalsIgnoreCase(forceLogging)) {
            throw ValidatorUtils.failedPrecondition(
                    "Oracle FORCE LOGGING must be enabled for LogMiner CDC");
        }
        if (supplementalLogDataMin == null || "NO".equalsIgnoreCase(supplementalLogDataMin)) {
            throw ValidatorUtils.failedPrecondition(
                    "Oracle minimal supplemental logging must be enabled for LogMiner CDC");
        }
    }

    private void validatePdb() throws SQLException {
        try (var stmt =
                jdbcConnection.prepareStatement(
                        "SELECT OPEN_MODE FROM V$PDBS WHERE UPPER(NAME) = ?")) {
            stmt.setString(1, pdbName);
            try (var result = stmt.executeQuery()) {
                if (!result.next()) {
                    throw ValidatorUtils.invalidArgument(
                            String.format("Oracle PDB '%s' does not exist", pdbName));
                }
                var openMode = result.getString("OPEN_MODE");
                if (!"READ WRITE".equalsIgnoreCase(openMode)) {
                    throw ValidatorUtils.failedPrecondition(
                            String.format(
                                    "Oracle PDB '%s' must be open READ WRITE, but is '%s'",
                                    pdbName, openMode));
                }
            }
        }
    }

    @Override
    void validateUserPrivilege() {
        try {
            var privileges = querySingleColumn("SELECT PRIVILEGE FROM SESSION_PRIVS");
            validateRequiredGrants("privileges", REQUIRED_PRIVILEGES, privileges);

            var roles = querySingleColumn("SELECT ROLE FROM SESSION_ROLES");
            validateRequiredGrants("roles", REQUIRED_ROLES, roles);
        } catch (SQLException e) {
            throw ValidatorUtils.internalError(e.getMessage());
        }
    }

    private Set<String> querySingleColumn(String sql) throws SQLException {
        var values = new HashSet<String>();
        try (var stmt = jdbcConnection.createStatement();
                var result = stmt.executeQuery(sql)) {
            while (result.next()) {
                values.add(result.getString(1).toUpperCase(Locale.ROOT));
            }
        }
        return values;
    }

    static void validateRequiredGrants(
            String grantType, Set<String> required, Set<String> granted) {
        var missing = new HashSet<>(required);
        missing.removeAll(granted);
        if (!missing.isEmpty()) {
            throw ValidatorUtils.invalidArgument(
                    String.format("Oracle user is missing required %s: %s", grantType, missing));
        }
    }

    @Override
    void validateTable() {
        try {
            switchToPdb();
            validateTableExists();
            validateTableSupplementalLogging();
        } catch (SQLException e) {
            throw ValidatorUtils.internalError(e.getMessage());
        }
    }

    private void switchToPdb() throws SQLException {
        try (var stmt = jdbcConnection.createStatement()) {
            stmt.execute("ALTER SESSION SET CONTAINER = " + pdbName);
        }
    }

    private void validateTableExists() throws SQLException {
        try (var stmt =
                jdbcConnection.prepareStatement(
                        "SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER = ? AND TABLE_NAME = ?")) {
            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);
            try (var result = stmt.executeQuery()) {
                result.next();
                if (result.getInt(1) == 0) {
                    throw ValidatorUtils.invalidArgument(
                            String.format(
                                    "Oracle table '%s.%s' does not exist in PDB '%s'",
                                    schemaName, tableName, pdbName));
                }
            }
        }
    }

    private void validateTableSupplementalLogging() throws SQLException {
        try (var stmt =
                jdbcConnection.prepareStatement(
                        "SELECT COUNT(*) FROM ALL_LOG_GROUPS "
                                + "WHERE OWNER = ? AND TABLE_NAME = ?")) {
            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);
            try (var result = stmt.executeQuery()) {
                result.next();
                if (result.getInt(1) == 0) {
                    throw ValidatorUtils.failedPrecondition(
                            String.format(
                                    "Oracle supplemental logging must be enabled on table '%s.%s'",
                                    schemaName, tableName));
                }
            }
        }
    }

    static String normalizePdbName(String pdbName) {
        if (pdbName == null || !ORACLE_UNQUOTED_IDENTIFIER.matcher(pdbName).matches()) {
            throw ValidatorUtils.invalidArgument(
                    "'database.pdb.name' must be an unquoted Oracle identifier");
        }
        return pdbName.toUpperCase(Locale.ROOT);
    }

    @Override
    boolean isCdcSourceJob() {
        return isCdcSourceJob;
    }

    @Override
    public void close() throws SQLException {
        jdbcConnection.close();
    }
}
