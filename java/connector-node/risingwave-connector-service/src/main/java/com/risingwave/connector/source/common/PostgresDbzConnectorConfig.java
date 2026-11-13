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

public class PostgresDbzConnectorConfig extends DbzConnectorConfig {
    public static final String SLOT_NAME = "slot.name";
    public static final String PUB_NAME = "publication.name";
    public static final String PUB_CREATE = "publication.create.enable";
    public static final String SSL_ROOT_CERT = "ssl.root.cert";
    public static final String IS_AWS_RDS = "postgres.is.aws.rds";
    public static final String TEST_ONLY_FORCE_RDS = "test.only.force.rds";

    PostgresDbzConnectorConfig(CdcSource source) {
        super(source);
    }

    @Override
    protected void customizeDebeziumProperties(Properties dbzProps) {
        var postgresProps = loadPropertiesTemplate("postgres.properties");

        var pubAutoCreate = Boolean.parseBoolean(userProps.getOrDefault(PUB_CREATE, "true"));
        if (!pubAutoCreate) {
            postgresProps.setProperty("publication.autocreate.mode", "disabled");
        }
        if (isBackfillSource) {
            // skip the initial snapshot for cdc backfill
            postgresProps.setProperty("snapshot.mode", "no_data");
            if (hasStartOffset()) {
                postgresProps.setProperty(
                        ConfigurableOffsetBackingStore.OFFSET_STATE_VALUE, startOffset);
            }
        } else if (snapshotDone && hasStartOffset()) {
            // if snapshot phase is finished and offset is specified, we will continue reading
            // changes from the given offset
            postgresProps.setProperty("snapshot.mode", "no_data");
            postgresProps.setProperty(
                    ConfigurableOffsetBackingStore.OFFSET_STATE_VALUE, startOffset);
        }

        // adapt value of sslmode to the expected value
        var sslMode = postgresProps.getProperty("database.sslmode");
        if (sslMode != null) {
            switch (sslMode) {
                case "disabled":
                    sslMode = "disable";
                    break;
                case "preferred":
                    sslMode = "prefer";
                    break;
                case "required":
                    sslMode = "require";
                    break;
            }
            postgresProps.setProperty("database.sslmode", sslMode);
        }

        dbzProps.putAll(postgresProps);

        if (userProps.containsKey(SSL_ROOT_CERT)) {
            dbzProps.setProperty("database.sslrootcert", userProps.get(SSL_ROOT_CERT));
        }
    }
}
