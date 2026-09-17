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

package com.risingwave.connector.source.oracle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.risingwave.connector.source.common.DbzConnectorConfig;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class OracleHeartbeatTableTest extends OracleSourceTestBase {
    private static OracleTestFixture oracle;

    @BeforeClass
    public static void startServices() throws Exception {
        oracle = new OracleTestFixture();
        oracle.start();
    }

    @AfterClass
    public static void stopServices() throws Exception {
        if (oracle != null) {
            oracle.close();
        }
    }

    @Override
    protected OracleTestFixture oracle() {
        return oracle;
    }

    @Test
    public void doesNotRequireOracleHeartbeatTableForZeroInterval() {
        createSourceTableWithAllColumnLogging();
        var properties = oracle.sourceProperties();
        properties.put(DbzConnectorConfig.HEARTBEAT_INTERVAL_KEY, "0");
        assertValid(properties);
    }

    @Test
    public void preservesExistingHeartbeatValue() throws SQLException {
        createSourceTableWithAllColumnLogging();
        var heartbeatTable = "C##RW_VALIDATOR.RW_HEARTBEAT";
        createHeartbeatTable(heartbeatTable, false);
        execute("INSERT INTO " + heartbeatTable + " VALUES (1, 7)");

        assertValid(autoInitializeProperties(heartbeatTable));

        assertEquals(7, queryInt("SELECT HEARTBEAT FROM " + heartbeatTable + " WHERE ID = 1"));
    }

    @Test
    public void reusesPrecreatedTableWithoutCreatePrivilege() throws SQLException {
        createSourceTableWithAllColumnLogging();
        createHeartbeatTable("APP.RW_HEARTBEAT", true);
        execute("GRANT UPDATE ON APP.RW_HEARTBEAT TO C##RW_VALIDATOR");

        assertValid(autoInitializeProperties("APP.RW_HEARTBEAT"));

        assertEquals(1, queryInt("SELECT COUNT(*) FROM APP.RW_HEARTBEAT WHERE ID = 1"));
    }

    @Test
    public void doesNotRepairIncompatibleExistingTable() throws SQLException {
        createSourceTableWithAllColumnLogging();
        var heartbeatTable = "C##RW_VALIDATOR.RW_HEARTBEAT";
        createTable(
                heartbeatTable, "CREATE TABLE " + heartbeatTable + " (ID NUMBER(1) PRIMARY KEY)");

        var error = validate(autoInitializeProperties(heartbeatTable)).getError().getErrorMessage();

        assertTrue(error, error.contains("must contain NUMBER columns"));
        assertTrue(error, error.contains("without overwriting its data"));
        assertEquals(
                0,
                queryInt(
                        "SELECT COUNT(*) FROM ALL_TAB_COLUMNS WHERE OWNER = 'C##RW_VALIDATOR' "
                                + "AND TABLE_NAME = 'RW_HEARTBEAT' AND COLUMN_NAME = 'HEARTBEAT'"));
    }

    @Test
    public void reportsSeedInsertFailureAndCanReuseManualSetup() throws SQLException {
        createSourceTableWithAllColumnLogging();
        var heartbeatTable = "APP.RW_AUTO_HEARTBEAT";
        createHeartbeatTable(heartbeatTable, false);
        var properties = autoInitializeProperties(heartbeatTable);

        var error = validate(properties).getError().getErrorMessage();
        assertTrue(error, error.contains("Failed to insert the seed row"));
        assertTrue(error, error.contains("INSERT INTO APP.RW_AUTO_HEARTBEAT"));
        assertEquals(0, queryInt("SELECT COUNT(*) FROM " + heartbeatTable));

        execute("INSERT INTO " + heartbeatTable + " VALUES (1, 0)");
        execute("GRANT UPDATE ON " + heartbeatTable + " TO C##RW_VALIDATOR");
        assertValid(properties);
        assertEquals(1, queryInt("SELECT COUNT(*) FROM " + heartbeatTable));
    }

    @Test
    public void concurrentInitializersCreateOneTableAndSeedRow() throws Exception {
        createSourceTableWithAllColumnLogging();
        var heartbeatTable = "C##RW_VALIDATOR.RW_HEARTBEAT";
        trackTable(heartbeatTable);

        runConcurrentValidations(autoInitializeProperties(heartbeatTable));

        assertEquals(1, queryInt("SELECT COUNT(*) FROM " + heartbeatTable + " WHERE ID = 1"));
    }

    @Test
    public void concurrentInitializersInsertOneSeedRow() throws Exception {
        createSourceTableWithAllColumnLogging();
        var heartbeatTable = "C##RW_VALIDATOR.RW_HEARTBEAT";
        createHeartbeatTable(heartbeatTable, false);

        runConcurrentValidations(autoInitializeProperties(heartbeatTable));

        assertEquals(1, queryInt("SELECT COUNT(*) FROM " + heartbeatTable + " WHERE ID = 1"));
    }

    @Test
    public void doesNotCreateMissingSchema() throws SQLException {
        createSourceTableWithAllColumnLogging();
        var error =
                validate(autoInitializeProperties("MISSING_SCHEMA.RW_HEARTBEAT"))
                        .getError()
                        .getErrorMessage();

        assertTrue(error, error.contains("schema 'MISSING_SCHEMA' does not exist"));
        assertTrue(error, error.contains("CREATE TABLE MISSING_SCHEMA.RW_HEARTBEAT"));
        assertEquals(
                0, queryInt("SELECT COUNT(*) FROM ALL_USERS WHERE USERNAME = 'MISSING_SCHEMA'"));
    }

    @Test
    public void returnsSetupSqlWhenTableCreationIsNotPermitted() throws SQLException {
        createSourceTableWithAllColumnLogging();
        var heartbeatTable = "APP.RW_AUTO_HEARTBEAT";
        trackTable(heartbeatTable);

        var error = validate(autoInitializeProperties(heartbeatTable)).getError().getErrorMessage();

        assertTrue(error, error.contains("Failed to create Oracle heartbeat table"));
        assertTrue(error, error.contains("CREATE TABLE APP.RW_AUTO_HEARTBEAT"));
        assertTrue(error, error.contains("GRANT UPDATE (HEARTBEAT) ON APP.RW_AUTO_HEARTBEAT"));
        assertEquals(
                0,
                queryInt(
                        "SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER = 'APP' "
                                + "AND TABLE_NAME = 'RW_AUTO_HEARTBEAT'"));
    }

    @Test
    public void acceptsHeartbeatTableUpdateGrant() {
        var properties = createHeartbeatSource();
        execute("GRANT UPDATE ON APP.RW_HEARTBEAT TO C##RW_VALIDATOR");
        assertValid(properties);
    }

    @Test
    public void acceptsHeartbeatColumnUpdateGrant() {
        var properties = createHeartbeatSource();
        execute("GRANT UPDATE (HEARTBEAT) ON APP.RW_HEARTBEAT TO C##RW_VALIDATOR");
        assertValid(properties);
    }

    @Test
    public void acceptsHeartbeatUpdateThroughRole() {
        var properties = createHeartbeatSource();
        execute("GRANT UPDATE ON APP.RW_HEARTBEAT TO HEARTBEAT_WRITER");
        assertValid(properties);
    }

    @Test
    public void rejectsHeartbeatWithoutUpdateGrant() {
        assertHeartbeatUpdateRejected(createHeartbeatSource());
    }

    @Test
    public void rejectsHeartbeatUpdateGrantOnWrongColumn() {
        var properties = createHeartbeatSource();
        execute("GRANT UPDATE (ID) ON APP.RW_HEARTBEAT TO C##RW_VALIDATOR");
        assertHeartbeatUpdateRejected(properties);
    }

    private Map<String, String> createHeartbeatSource() {
        createSourceTableWithAllColumnLogging();
        createHeartbeatTable("APP.RW_HEARTBEAT", true);
        var properties = oracle.sourceProperties();
        properties.put(DbzConnectorConfig.HEARTBEAT_INTERVAL_KEY, "300000");
        properties.put(DbzConnectorConfig.ORACLE_HEARTBEAT_TABLE_NAME, "APP.RW_HEARTBEAT");
        return properties;
    }

    protected void createHeartbeatTable(String name, boolean withSeedRow) {
        createTable(
                name,
                "CREATE TABLE "
                        + name
                        + " (ID NUMBER(1) PRIMARY KEY, HEARTBEAT NUMBER(1) NOT NULL)");
        if (withSeedRow) {
            execute("INSERT INTO " + name + " VALUES (1, 0)");
        }
    }

    private Map<String, String> autoInitializeProperties(String heartbeatTable) {
        var properties = oracle.sourceProperties();
        properties.put(DbzConnectorConfig.HEARTBEAT_INTERVAL_KEY, "1");
        properties.put(DbzConnectorConfig.ORACLE_HEARTBEAT_TABLE_NAME, heartbeatTable);
        properties.put(DbzConnectorConfig.HEARTBEAT_TABLE_AUTO_INITIALIZE_KEY, "true");
        return properties;
    }

    private void runConcurrentValidations(Map<String, String> properties) throws Exception {
        var barrier = new CyclicBarrier(2);
        var executor =
                Executors.newFixedThreadPool(
                        2,
                        runnable -> {
                            var thread = new Thread(runnable, "oracle-validation");
                            thread.setDaemon(true);
                            return thread;
                        });
        Future<?> first = null;
        Future<?> second = null;
        try {
            first =
                    executor.submit(
                            () -> {
                                barrier.await(30, TimeUnit.SECONDS);
                                assertValid(properties);
                                return null;
                            });
            second =
                    executor.submit(
                            () -> {
                                barrier.await(30, TimeUnit.SECONDS);
                                assertValid(properties);
                                return null;
                            });
            first.get(45, TimeUnit.SECONDS);
            second.get(45, TimeUnit.SECONDS);
        } finally {
            if (first != null) {
                first.cancel(true);
            }
            if (second != null) {
                second.cancel(true);
            }
            executor.shutdownNow();
        }
    }

    private void assertHeartbeatUpdateRejected(Map<String, String> properties) {
        var error = validate(properties).getError().getErrorMessage();
        assertTrue(
                error,
                error.startsWith(
                        "INVALID_ARGUMENT: Oracle user 'C##RW_VALIDATOR' needs UPDATE permission "
                                + "on heartbeat table 'APP.RW_HEARTBEAT'"));
        assertTrue(
                error,
                error.contains(
                        "GRANT UPDATE (HEARTBEAT) ON APP.RW_HEARTBEAT TO "
                                + "\"C##RW_VALIDATOR\""));
    }
}
