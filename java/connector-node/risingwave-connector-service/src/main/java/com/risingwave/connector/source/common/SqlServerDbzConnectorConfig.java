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
import com.risingwave.connector.cdc.debezium.internal.ConfigurableOffsetBackingStore;
import com.risingwave.proto.ConnectorServiceProto.GetEventStreamRequest;
import java.util.Properties;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

public class SqlServerDbzConnectorConfig extends DbzConnectorConfig {
    public static final String ENCRYPT = "database.encrypt";

    SqlServerDbzConnectorConfig(GetEventStreamRequest request) {
        super(request);
    }

    public String getJdbcUrl() {
        var encrypt = Boolean.parseBoolean(userProps.getOrDefault(ENCRYPT, "false"));
        return String.format(
                "jdbc:sqlserver://%s:%s;databaseName=%s;encrypt=%s;trustServerCertificate=true",
                getHost(), getPort(), getDbName(), encrypt);
    }

    @Override
    public void validateSource(TableSchema tableSchema, boolean isBackfillTable) throws Exception {
        ensureRequiredProps();
        ensurePropNotBlank(SCHEMA_NAME);
        try (var validator = new SqlServerValidator(this, tableSchema)) {
            validator.validateAll();
        }
    }

    /** The SQL Server connector registers its streaming metrics per task. */
    @Override
    protected ObjectName getStreamingMetricsObjectName(String dbServerName)
            throws MalformedObjectNameException {
        // TODO: fulfill the task id here, by WKX
        return new ObjectName(
                "debezium.sql_server:type=connector-metrics,task=0,context=streaming,server="
                        + dbServerName);
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
