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

import com.risingwave.connector.cdc.debezium.internal.ConfigurableOffsetBackingStore;
import io.debezium.heartbeat.DatabaseHeartbeatImpl;
import java.util.Properties;
import java.util.Set;

public class OracleDbzConnectorConfig extends DbzConnectorConfig {
    public static final String PDB_NAME = "database.pdb.name";
    public static final String HEARTBEAT_TABLE_NAME = "heartbeat.table.name";
    public static final String HEARTBEAT_ACTION_QUERY_KEY =
            DBZ_PROPERTY_PREFIX + DatabaseHeartbeatImpl.HEARTBEAT_ACTION_QUERY_PROPERTY_NAME;
    public static final String DEBEZIUM_RAC_NODES = "debezium.rac.nodes";
    public static final String CONNECTION_ADAPTER_KEY =
            DBZ_PROPERTY_PREFIX + "database.connection.adapter";
    public static final Set<String> SUPPORTED_CONNECTION_ADAPTERS =
            Set.of("logminer", "logminer_unbuffered");

    OracleDbzConnectorConfig(CdcSource source) {
        super(source);
    }

    @Override
    protected void customizeDebeziumProperties(Properties dbzProps) {
        var oracleProps = loadPropertiesTemplate("oracle.properties");
        // RisingWave backfills each table on demand, so the shared Debezium connector only
        // captures schemas and streams changes. On recovery, rebuild the in-memory schema
        // history and resume from the opaque offset persisted in the CDC split.
        // Include transactions that were already open when the shared source started.
        // Debezium defines this property with an internal. prefix.
        oracleProps.setProperty("internal.log.mining.transaction.snapshot.boundary.mode", "all");
        if (hasStartOffset()) {
            oracleProps.setProperty("snapshot.mode", "recovery");
            oracleProps.setProperty(ConfigurableOffsetBackingStore.OFFSET_STATE_VALUE, startOffset);
        } else {
            oracleProps.setProperty("snapshot.mode", "no_data");
        }
        var heartbeatTable = OracleHeartbeatTable.parse(userProps.get(HEARTBEAT_TABLE_NAME));
        oracleProps.setProperty(
                DatabaseHeartbeatImpl.HEARTBEAT_ACTION_QUERY_PROPERTY_NAME,
                heartbeatTable.actionQuery());
        dbzProps.putAll(oracleProps);
    }
}
