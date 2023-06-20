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

package com.risingwave.connector.source.common;

import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.connector.cdc.debezium.internal.ConfigurableOffsetBackingStore;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.io.IOUtils;
import org.apache.commons.text.StringSubstitutor;

public class DbzConnectorConfig {

    /* Common configs */
    public static final String HOST = "hostname";
    public static final String PORT = "port";
    public static final String USER = "username";
    public static final String PASSWORD = "password";

    public static final String DB_NAME = "database.name";
    public static final String TABLE_NAME = "table.name";

    public static final String DB_SERVERS = "database.servers";

    /* MySQL specified configs */
    public static final String MYSQL_SERVER_ID = "server.id";

    /* Postgres specified configs */
    public static final String PG_SLOT_NAME = "slot.name";
    public static final String PG_SCHEMA_NAME = "schema.name";

    private static final String DBZ_CONFIG_FILE = "debezium.properties";
    private static final String MYSQL_CONFIG_FILE = "mysql.properties";
    private static final String POSTGRES_CONFIG_FILE = "postgres.properties";

    public static Map<String, String> extractDebeziumProperties(Map<String, String> properties) {
        // retain only debezium properties if any
        var userProps = new HashMap<>(properties);
        userProps.remove(DbzConnectorConfig.HOST);
        userProps.remove(DbzConnectorConfig.PORT);
        userProps.remove(DbzConnectorConfig.USER);
        userProps.remove(DbzConnectorConfig.PASSWORD);
        userProps.remove(DbzConnectorConfig.DB_NAME);
        userProps.remove(DbzConnectorConfig.TABLE_NAME);
        userProps.remove(DbzConnectorConfig.MYSQL_SERVER_ID);
        userProps.remove(DbzConnectorConfig.PG_SLOT_NAME);
        userProps.remove(DbzConnectorConfig.PG_SCHEMA_NAME);
        return userProps;
    }

    private final long sourceId;

    private final SourceTypeE sourceType;

    private final Properties resolvedDbzProps;

    public long getSourceId() {
        return sourceId;
    }

    public SourceTypeE getSourceType() {
        return sourceType;
    }

    public Properties getResolvedDebeziumProps() {
        return resolvedDbzProps;
    }

    public DbzConnectorConfig(
            SourceTypeE source,
            long sourceId,
            String startOffset,
            Map<String, String> userProps,
            boolean snapshotDone) {
        StringSubstitutor substitutor = new StringSubstitutor(userProps);
        var dbzProps = initiateDbConfig(DBZ_CONFIG_FILE, substitutor);
        if (source == SourceTypeE.MYSQL) {
            var mysqlProps = initiateDbConfig(MYSQL_CONFIG_FILE, substitutor);
            // if snapshot phase is finished and offset is specified, we will continue binlog
            // reading from the given offset
            if (snapshotDone && null != startOffset && !startOffset.isBlank()) {
                // 'snapshot.mode=schema_only_recovery' must be configured if binlog offset is
                // specified.
                // It only snapshots the schemas, not the data, and continue binlog reading from the
                // specified offset
                mysqlProps.setProperty("snapshot.mode", "schema_only_recovery");
                mysqlProps.setProperty(
                        ConfigurableOffsetBackingStore.OFFSET_STATE_VALUE, startOffset);
            }

            dbzProps.putAll(mysqlProps);
        } else if (source == SourceTypeE.POSTGRES || source == SourceTypeE.CITUS) {
            var postgresProps = initiateDbConfig(POSTGRES_CONFIG_FILE, substitutor);

            // citus needs all_tables publication to capture all shards
            if (source == SourceTypeE.CITUS) {
                postgresProps.setProperty("publication.autocreate.mode", "all_tables");
            }

            // if snapshot phase is finished adn offset is specified, we will continue reading
            // changes from the given offset
            if (snapshotDone && null != startOffset && !startOffset.isBlank()) {
                postgresProps.setProperty("snapshot.mode", "never");
                postgresProps.setProperty(
                        ConfigurableOffsetBackingStore.OFFSET_STATE_VALUE, startOffset);
            }
            dbzProps.putAll(postgresProps);
        } else {
            throw new RuntimeException("unsupported source type: " + source);
        }

        var otherProps = extractDebeziumProperties(userProps);
        dbzProps.putAll(otherProps);

        this.sourceId = sourceId;
        this.sourceType = source;
        this.resolvedDbzProps = dbzProps;
    }

    private Properties initiateDbConfig(String fileName, StringSubstitutor substitutor) {
        var dbProps = new Properties();
        try (var input = getClass().getClassLoader().getResourceAsStream(fileName)) {
            assert input != null;
            var inputStr = IOUtils.toString(input, StandardCharsets.UTF_8);
            var resolvedStr = substitutor.replace(inputStr);
            dbProps.load(new StringReader(resolvedStr));
        } catch (IOException e) {
            throw new RuntimeException("failed to load config file " + fileName, e);
        }
        return dbProps;
    }
}
