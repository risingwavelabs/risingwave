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

package com.risingwave.sourcenode.mysql;

import com.risingwave.connector.api.source.ConnectorConfig;
import com.risingwave.connector.api.source.SourceConfig;
import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.connector.cdc.debezium.internal.ConfigurableOffsetBackingStore;
import com.risingwave.sourcenode.common.DebeziumCdcUtils;
import java.util.Map;
import java.util.Properties;

/** MySQL Source Config */
public class MySqlSourceConfig implements SourceConfig {
    static final String DB_SERVER_NAME_PREFIX = "RW_CDC_";
    private final Properties props = DebeziumCdcUtils.createCommonConfig();
    private final long id;
    private final String sourceName;

    public MySqlSourceConfig(long sourceId, String startOffset, Map<String, String> userProps) {
        id = sourceId;
        props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
        props.setProperty(
                "offset.storage", ConfigurableOffsetBackingStore.class.getCanonicalName());

        props.setProperty(
                "database.history", "io.debezium.relational.history.MemoryDatabaseHistory");
        // if offset is specified, we will continue binlog reading from the specified offset
        if (null != startOffset && !startOffset.isBlank()) {
            // 'snapshot.mode=schema_only_recovery' must be configured if binlog offset is
            // specified.
            // It only snapshots the schemas, not the data, and continue binlog reading from the
            // specified offset
            props.setProperty("snapshot.mode", "schema_only_recovery");
            props.setProperty(ConfigurableOffsetBackingStore.OFFSET_STATE_VALUE, startOffset);
        }

        // Begin of connector configs
        props.setProperty("database.hostname", userProps.get(ConnectorConfig.HOST));
        props.setProperty("database.port", userProps.get(ConnectorConfig.PORT));
        props.setProperty("database.user", userProps.get(ConnectorConfig.USER));
        props.setProperty("database.password", userProps.get(ConnectorConfig.PASSWORD));

        props.setProperty("database.include.list", userProps.get(ConnectorConfig.DB_NAME));
        // only captures data of the specified table
        String tableFilter =
                userProps.get(ConnectorConfig.DB_NAME)
                        + "."
                        + userProps.get(ConnectorConfig.TABLE_NAME);
        props.setProperty("table.include.list", tableFilter);

        // disable schema change events for current stage
        props.setProperty("include.schema.changes", "false");

        // ServerId must be unique since the connector will join the mysql cluster as a client.
        // By default, we generate serverId by adding a fixed number to the sourceId generated by
        // Meta. We may allow user to specify the ID in the future.
        props.setProperty("database.server.id", userProps.get(ConnectorConfig.MYSQL_SERVER_ID));
        props.setProperty("database.server.name", DB_SERVER_NAME_PREFIX + tableFilter);

        // host:port:database.table
        sourceName =
                userProps.get(ConnectorConfig.HOST)
                        + ":"
                        + userProps.get(ConnectorConfig.PORT)
                        + ":"
                        + userProps.get(ConnectorConfig.DB_NAME)
                        + "."
                        + userProps.get(ConnectorConfig.TABLE_NAME);

        props.setProperty("name", sourceName);

        // pass through debezium properties if any
        var dbzProperties = ConnectorConfig.extractDebeziumProperties(userProps);
        props.putAll(dbzProperties);
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public String getSourceName() {
        return sourceName;
    }

    @Override
    public SourceTypeE getSourceType() {
        return SourceTypeE.MYSQL;
    }

    @Override
    public Properties getProperties() {
        return props;
    }
}
