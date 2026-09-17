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

import com.risingwave.connector.source.common.DbzConnectorConfig;
import java.util.Map;
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

    private void assertHeartbeatUpdateRejected(Map<String, String> properties) {
        assertEquals(
                "INVALID_ARGUMENT: Oracle user 'C##RW_VALIDATOR' needs UPDATE permission on "
                        + "heartbeat table 'APP.RW_HEARTBEAT'",
                validate(properties).getError().getErrorMessage());
    }
}
