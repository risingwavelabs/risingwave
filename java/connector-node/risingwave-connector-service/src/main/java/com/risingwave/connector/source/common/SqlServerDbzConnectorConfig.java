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
import java.util.Properties;

public class SqlServerDbzConnectorConfig extends DbzConnectorConfig {
    public static final String ENCRYPT = "database.encrypt";

    SqlServerDbzConnectorConfig(CdcSource source) {
        super(source);
    }

    @Override
    protected void customizeDebeziumProperties(Properties dbzProps) {
        var sqlServerProps = loadPropertiesTemplate("sql_server.properties");
        sqlServerProps.setProperty("snapshot.locking.mode", "none");

        if (isBackfillSource) {
            if (hasStartOffset()) {
                // skip the initial snapshot for cdc backfill and continue reading changes from
                // the given offset
                sqlServerProps.setProperty("snapshot.mode", "recovery");
                sqlServerProps.setProperty(
                        ConfigurableOffsetBackingStore.OFFSET_STATE_VALUE, startOffset);
            } else {
                sqlServerProps.setProperty("snapshot.mode", "no_data");
            }
        } else if (snapshotDone && hasStartOffset()) {
            // if snapshot phase is finished and offset is specified, we will continue reading
            // changes from the given offset
            sqlServerProps.setProperty("snapshot.mode", "recovery");
            sqlServerProps.setProperty(
                    ConfigurableOffsetBackingStore.OFFSET_STATE_VALUE, startOffset);
        }
        dbzProps.putAll(sqlServerProps);
    }
}
