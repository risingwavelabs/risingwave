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
import io.debezium.config.CommonConnectorConfig;
import java.lang.management.ManagementFactory;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import javax.management.JMException;
import javax.management.ObjectName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlServerDbzConnectorConfig extends DbzConnectorConfig {
    private static final Logger LOG = LoggerFactory.getLogger(SqlServerDbzConnectorConfig.class);

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

    /** Same as the default, but the SQL Server connector registers its metrics per task. */
    @Override
    public boolean waitForStreamingRunning(BooleanSupplier shouldContinue)
            throws InterruptedException {
        var dbServerName =
                getResolvedDebeziumProps().getProperty(CommonConnectorConfig.TOPIC_PREFIX.name());
        LOG.info("Waiting for streaming source of {} to start", dbServerName);
        var mbeanServer = ManagementFactory.getPlatformMBeanServer();
        int pollCount = 0;
        while (true) {
            if (!shouldContinue.getAsBoolean()) {
                LOG.info("Cancelled waiting for streaming source of {} to start", dbServerName);
                return false;
            }
            try {
                // TODO: fulfill the task id here, by WKX
                var metricsName =
                        new ObjectName(
                                "debezium.sql_server:type=connector-metrics,task=0,context=streaming,server="
                                        + dbServerName);
                if ((boolean) mbeanServer.getAttribute(metricsName, "Connected")) {
                    LOG.info("Debezium streaming source of {} started", dbServerName);
                    return true;
                }
            } catch (JMException _ex) {
                // expected while the streaming source is not ready
            }
            if (pollCount > waitStreamingStartTimeout) {
                LOG.error(
                        "Debezium streaming source of {} failed to start in timeout {}",
                        dbServerName,
                        waitStreamingStartTimeout);
                return false;
            }
            TimeUnit.SECONDS.sleep(1);
            pollCount++;
        }
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
