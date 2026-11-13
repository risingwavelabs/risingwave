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
import com.risingwave.connector.cdc.debezium.internal.OpendalSchemaHistory;
import java.util.Properties;

public class MysqlDbzConnectorConfig extends DbzConnectorConfig {
    public static final String SERVER_ID = "server.id";
    public static final String SSL_MODE = "ssl.mode";

    MysqlDbzConnectorConfig(CdcSource source) {
        super(source);
    }

    protected String getPropertiesTemplate() {
        return "mysql.properties";
    }

    @Override
    protected void customizeDebeziumProperties(Properties dbzProps) {
        var mysqlProps = loadPropertiesTemplate(getPropertiesTemplate());

        // Enable schema history for all MySQL CDC modes to handle schema changes properly
        mysqlProps.setProperty(OpendalSchemaHistory.SOURCE_ID, String.valueOf(sourceId));
        mysqlProps.setProperty(OpendalSchemaHistory.MAX_RECORDS_PER_FILE_CONFIG, "2048");

        if (isBackfillSource) {
            mysqlProps.setProperty("snapshot.locking.mode", "none");
            // If cdc backfill enabled, the source only emit incremental changes, so we must
            // rewind to the given offset and continue binlog reading from there
            if (hasStartOffset()) {
                mysqlProps.setProperty("snapshot.mode", "custom");
                mysqlProps.setProperty(
                        ConfigurableOffsetBackingStore.OFFSET_STATE_VALUE, startOffset);
            } else {
                // read upstream table schemas and emit incremental changes only
                mysqlProps.setProperty("snapshot.mode", "no_data");
            }
        } else if (snapshotDone && hasStartOffset()) {
            // if snapshot phase is finished and offset is specified, we will continue binlog
            // reading from the given offset. Since we use persistent schema history, we only
            // need to snapshot schema when no offset is passed, restore directly from schema
            // history when offset is available.
            mysqlProps.setProperty("snapshot.mode", "custom");
            mysqlProps.setProperty(ConfigurableOffsetBackingStore.OFFSET_STATE_VALUE, startOffset);
        }

        dbzProps.putAll(mysqlProps);
    }
}
