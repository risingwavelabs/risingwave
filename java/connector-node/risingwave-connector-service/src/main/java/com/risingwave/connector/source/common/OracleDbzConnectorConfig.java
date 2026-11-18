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

import com.risingwave.connector.api.TableSchema;
import com.risingwave.proto.ConnectorServiceProto.GetEventStreamRequest;
import io.debezium.heartbeat.DatabaseHeartbeatImpl;
import java.util.Properties;

public class OracleDbzConnectorConfig extends DbzConnectorConfig {
    public static final String PDB_NAME = "database.pdb.name";
    public static final String HEARTBEAT_TABLE_NAME = "heartbeat.table.name";
    public static final String HEARTBEAT_ACTION_QUERY_KEY =
            DBZ_PROPERTY_PREFIX + DatabaseHeartbeatImpl.HEARTBEAT_ACTION_QUERY_PROPERTY_NAME;

    OracleDbzConnectorConfig(GetEventStreamRequest request) {
        super(request);
    }

    public String getPdbName() {
        return userProps.get(PDB_NAME);
    }

    public String getHeartbeatTableName() {
        return userProps.get(HEARTBEAT_TABLE_NAME);
    }

    @Override
    public void validateSource(TableSchema tableSchema, boolean isBackfillTable) throws Exception {
        ensureRequiredProps();
        ensurePropNotBlank(PDB_NAME);
        ensurePropNotBlank(SCHEMA_NAME);
        ensurePropNotBlank(HEARTBEAT_TABLE_NAME);
        if (userProps.containsKey(HEARTBEAT_ACTION_QUERY_KEY)) {
            throw ValidatorUtils.invalidArgument(
                    String.format(
                            "'%s' is generated internally; configure '%s' instead",
                            HEARTBEAT_ACTION_QUERY_KEY, HEARTBEAT_TABLE_NAME));
        }
        try (var validator = new OracleValidator(this, tableSchema)) {
            validator.validateAll();
        }
    }

    @Override
    protected void customizeDebeziumProperties(Properties dbzProps) {
        var oracleProps = loadPropertiesTemplate("oracle.properties");
        var heartbeatTable = OracleHeartbeatTable.parse(getHeartbeatTableName());
        oracleProps.setProperty(
                DatabaseHeartbeatImpl.HEARTBEAT_ACTION_QUERY_PROPERTY_NAME,
                heartbeatTable.actionQuery());
        dbzProps.putAll(oracleProps);
    }
}
