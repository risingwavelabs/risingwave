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

import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.connector.cdc.debezium.internal.ConfigurableOffsetBackingStore;
import java.util.HashMap;
import org.junit.Test;

public class DbzConnectorConfigTest {
    @Test
    public void usesConstructorSourceIdForTopicPrefix() {
        var userProps = new HashMap<String, String>();
        userProps.put("mongodb.url", "mongodb://localhost:27017");
        userProps.put("collection.name", "test.users");
        userProps.put("source.id", "user-supplied-value");

        var config = new DbzConnectorConfig(SourceTypeE.MONGODB, 42, null, userProps, false, false);

        assertEquals("RW_CDC_42", config.getResolvedDebeziumProps().getProperty("topic.prefix"));
    }

    @Test
    public void usesNoDataSnapshotModeForSharedOracleSourceCreation() {
        var userProps = oracleProperties();
        userProps.put("debezium.snapshot.mode", "rw_cdc_backfill");
        var config = new DbzConnectorConfig(SourceTypeE.ORACLE, 42, null, userProps, false, true);

        assertEquals("no_data", config.getResolvedDebeziumProps().getProperty("snapshot.mode"));
    }

    @Test
    public void usesRecoverySnapshotModeAndOpaqueOffsetForSharedOracleSourceRecovery() {
        var userProps = oracleProperties();
        userProps.put("debezium.snapshot.mode", "rw_cdc_backfill");
        userProps.put("debezium.decimal.handling.mode", "precise");
        var offset =
                "{\"sourcePartition\":{\"server\":\"RW_CDC_42\"},"
                        + "\"sourceOffset\":{\"scn\":\"3134314\","
                        + "\"commit_scn\":\"3134315:1:8.30.1337\"},"
                        + "\"isHeartbeat\":false}";
        var config = new DbzConnectorConfig(SourceTypeE.ORACLE, 42, offset, userProps, false, true);
        var properties = config.getResolvedDebeziumProps();

        assertEquals("recovery", properties.getProperty("snapshot.mode"));
        assertEquals(
                offset, properties.getProperty(ConfigurableOffsetBackingStore.OFFSET_STATE_VALUE));
        assertEquals("string", properties.getProperty("decimal.handling.mode"));
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
}
