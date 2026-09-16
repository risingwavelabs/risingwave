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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.connector.source.SourceValidateHandler;
import com.risingwave.proto.ConnectorServiceProto;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class OracleConnectorConfigTest {
    private static final String PASSWORD = "RwTestPass123";
    private static GenericContainer<?> oracle;
    private Connection pdb;
    private boolean heartbeatTableCreated;

    @BeforeClass
    public static void startOracle() throws SQLException {
        // Keep the config tests runnable without Docker. Only the SQL integration tests opt in.
        if (!Boolean.parseBoolean(
                System.getProperties().getProperty("oracle.validator.integration"))) {
            return;
        }
        oracle =
                new GenericContainer<>(
                                System.getProperties()
                                        .getProperty(
                                                "oracle.test.image",
                                                "container-registry.oracle.com/database/free:23.9.0.0"))
                        .withEnv("ORACLE_PWD", PASSWORD)
                        .withExposedPorts(1521)
                        .waitingFor(Wait.forLogMessage(".*DATABASE IS READY TO USE!.*\\n", 1))
                        .withStartupTimeout(Duration.ofMinutes(10));
        oracle.start();
        try (var cdb = connect("FREE");
                var stmt = cdb.createStatement()) {
            stmt.execute("ALTER DATABASE ADD SUPPLEMENTAL LOG DATA");
            stmt.execute(
                    "CREATE USER C##RW_VALIDATOR IDENTIFIED BY " + PASSWORD + " CONTAINER=ALL");
            stmt.execute(
                    "GRANT CREATE SESSION, SET CONTAINER, SELECT ANY TABLE, SELECT_CATALOG_ROLE "
                            + "TO C##RW_VALIDATOR CONTAINER=ALL");
        }
        try (var pdb = connect("FREEPDB1");
                var stmt = pdb.createStatement()) {
            stmt.execute("CREATE USER APP IDENTIFIED BY " + PASSWORD + " QUOTA UNLIMITED ON USERS");
            stmt.execute("CREATE ROLE HEARTBEAT_WRITER");
            stmt.execute("GRANT HEARTBEAT_WRITER TO C##RW_VALIDATOR");
        }
    }

    @AfterClass
    public static void stopOracle() {
        if (oracle != null) {
            oracle.stop();
            oracle = null;
        }
    }

    private static Connection connect(String service) throws SQLException {
        return DriverManager.getConnection(
                "jdbc:oracle:thin:@//"
                        + oracle.getHost()
                        + ":"
                        + oracle.getMappedPort(1521)
                        + "/"
                        + service,
                "SYSTEM",
                PASSWORD);
    }

    private void createTable() throws SQLException {
        assumeTrue("Enable with -Doracle.validator.integration=true", oracle != null);
        pdb = connect("FREEPDB1");
        execute(
                "CREATE TABLE APP.CUSTOMERS (ID NUMBER PRIMARY KEY, NAME VARCHAR2(100), EMAIL VARCHAR2(100))");
    }

    private void createHeartbeatTable() throws SQLException {
        createTable();
        execute("CREATE TABLE APP.RW_HEARTBEAT (ID NUMBER PRIMARY KEY, HEARTBEAT NUMBER)");
        heartbeatTableCreated = true;
        execute("INSERT INTO APP.RW_HEARTBEAT VALUES (1, 0)");
    }

    @After
    public void dropTable() throws SQLException {
        if (pdb != null) {
            try {
                if (heartbeatTableCreated) {
                    execute("DROP TABLE APP.RW_HEARTBEAT PURGE");
                }
                execute("DROP TABLE APP.CUSTOMERS PURGE");
            } finally {
                pdb.close();
            }
        }
    }

    private void execute(String sql) throws SQLException {
        try (var stmt = pdb.createStatement()) {
            stmt.execute(sql);
        }
    }

    private static Map<String, String> liveOracleProperties() {
        // Use a non-DBA connector user so catalog access is checked as well as SQL syntax.
        return Map.of(
                DbzConnectorConfig.HOST, oracle.getHost(),
                DbzConnectorConfig.PORT, oracle.getMappedPort(1521).toString(),
                DbzConnectorConfig.USER, "C##RW_VALIDATOR",
                DbzConnectorConfig.PASSWORD, PASSWORD,
                DbzConnectorConfig.DB_NAME, "FREE",
                DbzConnectorConfig.ORACLE_PDB_NAME, "FREEPDB1",
                DbzConnectorConfig.ORACLE_SCHEMA_NAME, "APP",
                DbzConnectorConfig.TABLE_NAME, "CUSTOMERS");
    }

    private void validateTable() throws SQLException {
        try (var validator = new OracleValidator(liveOracleProperties(), false)) {
            validator.validateTable();
        }
    }

    private void validateHeartbeatTable() throws SQLException {
        try (var validator = new OracleValidator(liveOracleProperties(), false)) {
            // Exercise the production SQL without requiring the unrelated LogMiner setup.
            validator.validateHeartbeatTable(OracleHeartbeatTable.parse("APP.RW_HEARTBEAT"));
        }
    }

    private void assertLoggingRejected() {
        var exception = assertThrows(StatusRuntimeException.class, this::validateTable);
        assertEquals(Status.Code.FAILED_PRECONDITION, exception.getStatus().getCode());
        assertTrue(
                exception.getStatus().getDescription().contains("all-column supplemental logging"));
    }

    @Test
    public void resolvesOracleCdbPdbAndLogMinerProperties() {
        var userProps = oracleProperties();
        userProps.put("debezium.database.connection.adapter", "xstream");

        var config = new DbzConnectorConfig(SourceTypeE.ORACLE, 42, null, userProps, false, false);
        var properties = config.getResolvedDebeziumProps();

        assertEquals(
                "io.debezium.connector.oracle.OracleConnector",
                properties.getProperty("connector.class"));
        assertEquals("db.example.com", properties.getProperty("database.hostname"));
        assertEquals("1521", properties.getProperty("database.port"));
        assertEquals("FREE", properties.getProperty("database.dbname"));
        assertEquals("FREEPDB1", properties.getProperty("database.pdb.name"));
        assertEquals("APP.CUSTOMERS", properties.getProperty("table.include.list"));
        assertEquals("logminer", properties.getProperty("database.connection.adapter"));
        assertEquals("online_catalog", properties.getProperty("log.mining.strategy"));
        assertEquals("initial", properties.getProperty("snapshot.mode"));
        assertEquals("RW_CDC_42", properties.getProperty("topic.prefix"));
        assertEquals("300000", properties.getProperty("heartbeat.interval.ms"));
        assertEquals(
                "UPDATE APP.RW_HEARTBEAT SET HEARTBEAT = CASE HEARTBEAT WHEN 0 THEN 1 ELSE 0 END "
                        + "WHERE ID = 1",
                properties.getProperty("heartbeat.action.query"));
    }

    @Test
    public void disablesOracleHeartbeatWhenIntervalIsAbsent() {
        var userProps = oracleProperties();
        userProps.remove(DbzConnectorConfig.HEARTBEAT_INTERVAL_KEY);
        userProps.remove(DbzConnectorConfig.ORACLE_HEARTBEAT_TABLE_NAME);

        var config = new DbzConnectorConfig(SourceTypeE.ORACLE, 42, null, userProps, false, false);

        assertFalse(config.getResolvedDebeziumProps().containsKey("heartbeat.interval.ms"));
        assertFalse(config.getResolvedDebeziumProps().containsKey("heartbeat.action.query"));
    }

    @Test
    public void disablesOracleHeartbeatWithZeroInterval() {
        var userProps = oracleProperties();
        userProps.put(DbzConnectorConfig.HEARTBEAT_INTERVAL_KEY, "0");
        userProps.remove(DbzConnectorConfig.ORACLE_HEARTBEAT_TABLE_NAME);

        assertFalse(DbzConnectorConfig.isHeartbeatEnabled(userProps));
        for (var isSourceJob : new boolean[] {false, true}) {
            var config =
                    new DbzConnectorConfig(
                            SourceTypeE.ORACLE, 42, null, userProps, false, isSourceJob);
            var properties = config.getResolvedDebeziumProps();
            assertEquals("0", properties.getProperty("heartbeat.interval.ms"));
            assertFalse(properties.containsKey("heartbeat.action.query"));
        }
    }

    @Test
    public void rejectsHeartbeatTableWithoutActiveOracleHeartbeat() {
        for (var interval : new String[] {null, "0"}) {
            var userProps = oracleProperties();
            if (interval == null) {
                userProps.remove(DbzConnectorConfig.HEARTBEAT_INTERVAL_KEY);
            } else {
                userProps.put(DbzConnectorConfig.HEARTBEAT_INTERVAL_KEY, interval);
            }
            var exception =
                    assertThrows(
                            StatusRuntimeException.class,
                            () ->
                                    SourceValidateHandler.validateSource(
                                            oracleValidateRequest(userProps)));
            assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
            assertEquals(
                    "'heartbeat.table.name' requires a positive 'debezium.heartbeat.interval.ms'",
                    exception.getStatus().getDescription());
        }
    }

    @Test
    public void requiresHeartbeatTableForPositiveOracleInterval() {
        var userProps = oracleProperties();
        userProps.remove(DbzConnectorConfig.ORACLE_HEARTBEAT_TABLE_NAME);
        var exception =
                assertThrows(
                        StatusRuntimeException.class,
                        () ->
                                SourceValidateHandler.validateSource(
                                        oracleValidateRequest(userProps)));
        assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
        assertEquals(
                "'heartbeat.table.name' is not found. Please check the WITH properties",
                exception.getStatus().getDescription());
    }

    @Test
    public void removesTableFilterForSharedOracleSource() {
        var config =
                new DbzConnectorConfig(
                        SourceTypeE.ORACLE, 42, null, oracleProperties(), false, true);

        assertFalse(config.getResolvedDebeziumProps().containsKey("table.include.list"));
    }

    @Test
    public void buildsOracleThinJdbcUrl() {
        assertEquals(
                "jdbc:oracle:thin:@//db.example.com:1521/FREE",
                ValidatorUtils.getJdbcUrl(SourceTypeE.ORACLE, "db.example.com", "1521", "FREE"));
    }

    @Test
    public void validatesOracleIdentifierAndRequiredGrants() {
        assertEquals("FREEPDB1", OracleValidator.normalizePdbName("freepdb1"));
        assertThrows(
                RuntimeException.class,
                () -> OracleValidator.normalizePdbName("FREEPDB1; DROP TABLE APP.CUSTOMERS"));

        OracleValidator.validateRequiredGrants(
                "privileges", Set.of("CREATE SESSION"), Set.of("CREATE SESSION"));
        assertThrows(
                RuntimeException.class,
                () ->
                        OracleValidator.validateRequiredGrants(
                                "privileges", Set.of("CREATE SESSION"), Set.of()));
    }

    @Test
    public void validatesOracleHeartbeatTableAndBuildsActionQuery() {
        var heartbeatTable = OracleHeartbeatTable.parse("app.rw_heartbeat");
        assertEquals("APP", heartbeatTable.owner());
        assertEquals("RW_HEARTBEAT", heartbeatTable.table());
        assertEquals("APP.RW_HEARTBEAT", heartbeatTable.qualifiedName());
        assertEquals(
                "UPDATE APP.RW_HEARTBEAT SET HEARTBEAT = CASE HEARTBEAT WHEN 0 THEN 1 ELSE 0 END "
                        + "WHERE ID = 1",
                heartbeatTable.actionQuery());

        assertThrows(RuntimeException.class, () -> OracleHeartbeatTable.parse("RW_HEARTBEAT"));
        assertThrows(
                RuntimeException.class,
                () -> OracleHeartbeatTable.parse("APP.RW_HEARTBEAT; DROP TABLE APP.CUSTOMERS"));
    }

    @Test
    public void rejectsUserProvidedOracleHeartbeatActionQuery() {
        var userProps = oracleProperties();
        userProps.put(
                DbzConnectorConfig.HEARTBEAT_ACTION_QUERY_KEY,
                "UPDATE APP.RW_HEARTBEAT SET HEARTBEAT = 1");
        var request = oracleValidateRequest(userProps);

        var exception =
                assertThrows(
                        StatusRuntimeException.class,
                        () -> SourceValidateHandler.validateSource(request));
        assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
        assertEquals(
                "'debezium.heartbeat.action.query' is generated internally; configure "
                        + "'heartbeat.table.name' instead",
                exception.getStatus().getDescription());
    }

    @Test
    public void rejectsNegativeOracleHeartbeatInterval() {
        for (var interval : new String[] {"-1", "-300000"}) {
            var userProps = oracleProperties();
            userProps.put(DbzConnectorConfig.HEARTBEAT_INTERVAL_KEY, interval);

            var exception =
                    assertThrows(
                            StatusRuntimeException.class,
                            () ->
                                    SourceValidateHandler.validateSource(
                                            oracleValidateRequest(userProps)));
            assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
            assertEquals(
                    "'debezium.heartbeat.interval.ms' must be an integer between 0 and 2147483647, got: '"
                            + interval
                            + "'",
                    exception.getStatus().getDescription());
        }
    }

    @Test
    public void validatesOracleSourceTableExistence() {
        OracleValidator.validateTableExists("Oracle table", "APP", "CUSTOMERS", "FREEPDB1", 1);

        var exception =
                assertThrows(
                        StatusRuntimeException.class,
                        () ->
                                OracleValidator.validateTableExists(
                                        "Oracle table", "APP", "CUSTOMERS", "FREEPDB1", 0));
        assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
        assertEquals(
                "Oracle table 'APP.CUSTOMERS' does not exist in PDB 'FREEPDB1'",
                exception.getStatus().getDescription());
    }

    @Test
    public void validatesOracleHeartbeatUpdatePrivilege() {
        assertFalse(
                OracleValidator.hasHeartbeatUpdatePrivilege(
                        "C##DBZUSER", "APP", Set.of(), Set.of(), Set.of()));
        assertTrue(
                OracleValidator.hasHeartbeatUpdatePrivilege(
                        "APP", "APP", Set.of(), Set.of(), Set.of()));
        assertTrue(
                OracleValidator.hasHeartbeatUpdatePrivilege(
                        "C##DBZUSER", "APP", Set.of("UPDATE ANY TABLE"), Set.of(), Set.of()));
        assertTrue(
                OracleValidator.hasHeartbeatUpdatePrivilege(
                        "C##DBZUSER", "APP", Set.of(), Set.of(), Set.of("C##DBZUSER")));
        assertTrue(
                OracleValidator.hasHeartbeatUpdatePrivilege(
                        "C##DBZUSER", "APP", Set.of(), Set.of("CDC_ROLE"), Set.of("CDC_ROLE")));
    }

    @Test
    public void validatesOracleLoggingConfiguration() {
        OracleValidator.validateLoggingConfiguration("ARCHIVELOG", "YES", "YES");

        assertThrows(
                RuntimeException.class,
                () -> OracleValidator.validateLoggingConfiguration("NOARCHIVELOG", "YES", "YES"));
        assertThrows(
                RuntimeException.class,
                () -> OracleValidator.validateLoggingConfiguration("ARCHIVELOG", "NO", "YES"));
        assertThrows(
                RuntimeException.class,
                () -> OracleValidator.validateLoggingConfiguration("ARCHIVELOG", "YES", "NO"));
    }

    @Test
    public void doesNotRequireOracleHeartbeatTableForZeroInterval() throws SQLException {
        createTable();
        var properties = new HashMap<>(liveOracleProperties());
        properties.put(DbzConnectorConfig.HEARTBEAT_INTERVAL_KEY, "0");
        // No heartbeat table exists or is configured. Construction must not try to parse its name.
        try (var validator = new OracleValidator(properties, false)) {
            execute("ALTER TABLE APP.CUSTOMERS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
            validator.validateTable();
        }
    }

    @Test
    public void acceptsHeartbeatTableUpdateGrant() throws SQLException {
        createHeartbeatTable();
        execute("GRANT UPDATE ON APP.RW_HEARTBEAT TO C##RW_VALIDATOR");
        validateHeartbeatTable();
    }

    @Test
    public void acceptsHeartbeatColumnUpdateGrant() throws SQLException {
        createHeartbeatTable();
        execute("GRANT UPDATE (HEARTBEAT) ON APP.RW_HEARTBEAT TO C##RW_VALIDATOR");
        validateHeartbeatTable();
    }

    @Test
    public void acceptsHeartbeatUpdateThroughRole() throws SQLException {
        createHeartbeatTable();
        execute("GRANT UPDATE ON APP.RW_HEARTBEAT TO HEARTBEAT_WRITER");
        validateHeartbeatTable();
    }

    @Test
    public void rejectsHeartbeatWithoutUpdateGrant() throws SQLException {
        createHeartbeatTable();
        assertHeartbeatUpdateRejected();
    }

    @Test
    public void rejectsHeartbeatUpdateGrantOnWrongColumn() throws SQLException {
        createHeartbeatTable();
        execute("GRANT UPDATE (ID) ON APP.RW_HEARTBEAT TO C##RW_VALIDATOR");
        assertHeartbeatUpdateRejected();
    }

    private void assertHeartbeatUpdateRejected() {
        var exception = assertThrows(StatusRuntimeException.class, this::validateHeartbeatTable);
        assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
        assertEquals(
                "Oracle user 'C##RW_VALIDATOR' needs UPDATE permission on heartbeat table 'APP.RW_HEARTBEAT'",
                exception.getStatus().getDescription());
    }

    @Test
    public void rejectsMissingLogging() throws SQLException {
        createTable();
        assertLoggingRejected();
    }

    @Test
    public void rejectsPrimaryKeyOnlyLogging() throws SQLException {
        createTable();
        execute("ALTER TABLE APP.CUSTOMERS ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS");
        assertLoggingRejected();
    }

    @Test
    public void rejectsPartialConditionalGroup() throws SQLException {
        createTable();
        execute("ALTER TABLE APP.CUSTOMERS ADD SUPPLEMENTAL LOG GROUP PARTIAL_LOG (NAME)");
        assertLoggingRejected();
    }

    @Test
    public void rejectsPartialUnconditionalGroup() throws SQLException {
        createTable();
        execute("ALTER TABLE APP.CUSTOMERS ADD SUPPLEMENTAL LOG GROUP PARTIAL_LOG (NAME) ALWAYS");
        assertLoggingRejected();
    }

    @Test
    public void acceptsTableAllColumnLogging() throws SQLException {
        createTable();
        execute("ALTER TABLE APP.CUSTOMERS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
        validateTable();
    }

    @Test
    public void rejectsAllColumnLoggingOnAnotherTable() throws SQLException {
        createTable();
        execute("CREATE TABLE APP.OTHER_TABLE (ID NUMBER)");
        try {
            execute("ALTER TABLE APP.OTHER_TABLE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
            assertLoggingRejected();
        } finally {
            execute("DROP TABLE APP.OTHER_TABLE PURGE");
        }
    }

    @Test
    public void acceptsPdbAllColumnLoggingWithoutTableGroup() throws SQLException {
        createTable();
        execute("ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
        try {
            // PDB-only ALL logging need not be reflected in the CDB-wide flag.
            try (var cdb = connect("FREE");
                    var stmt = cdb.createStatement();
                    var result =
                            stmt.executeQuery("SELECT SUPPLEMENTAL_LOG_DATA_ALL FROM V$DATABASE")) {
                assertTrue(result.next());
                assertEquals("NO", result.getString(1));
            }
            validateTable();
        } finally {
            execute("ALTER DATABASE DROP SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
        }
        assertLoggingRejected();
    }

    @Test
    public void acceptsCdbAllColumnLoggingWithoutTableGroup() throws SQLException {
        createTable();
        try (var cdb = connect("FREE");
                var stmt = cdb.createStatement()) {
            stmt.execute("ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
            try {
                validateTable();
            } finally {
                stmt.execute("ALTER DATABASE DROP SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
            }
        }
        assertLoggingRejected();
    }

    private static HashMap<String, String> oracleProperties() {
        var properties = new HashMap<String, String>();
        properties.put(DbzConnectorConfig.HOST, "db.example.com");
        properties.put(DbzConnectorConfig.PORT, "1521");
        properties.put(DbzConnectorConfig.USER, "C##DBZUSER");
        properties.put(DbzConnectorConfig.PASSWORD, "secret");
        properties.put(DbzConnectorConfig.DB_NAME, "FREE");
        properties.put(DbzConnectorConfig.ORACLE_PDB_NAME, "FREEPDB1");
        properties.put(DbzConnectorConfig.ORACLE_SCHEMA_NAME, "APP");
        properties.put(DbzConnectorConfig.TABLE_NAME, "CUSTOMERS");
        properties.put(DbzConnectorConfig.HEARTBEAT_INTERVAL_KEY, "300000");
        properties.put(DbzConnectorConfig.ORACLE_HEARTBEAT_TABLE_NAME, "APP.RW_HEARTBEAT");
        return properties;
    }

    private static ConnectorServiceProto.ValidateSourceRequest oracleValidateRequest(
            Map<String, String> properties) {
        return ConnectorServiceProto.ValidateSourceRequest.newBuilder()
                .setSourceType(ConnectorServiceProto.SourceType.ORACLE)
                .putAllProperties(properties)
                .build();
    }
}
