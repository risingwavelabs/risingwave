// Copyright 2025 RisingWave Labs
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

import com.mongodb.ConnectionString;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbzConnectorConfig {
    private static final Logger LOG = LoggerFactory.getLogger(DbzConnectorConfig.class);

    private static final String WAIT_FOR_STREAMING_START_TIMEOUT_SECS =
            "cdc.source.wait.streaming.start.timeout";

    /* Common configs */
    public static final String HOST = "hostname";
    public static final String PORT = "port";
    public static final String USER = "username";
    public static final String PASSWORD = "password";

    public static final String DB_NAME = "database.name";
    public static final String TABLE_NAME = "table.name";

    public static final String DB_SERVERS = "database.servers";

    /* MySQL configs */
    public static final String MYSQL_SERVER_ID = "server.id";
    public static final String MYSQL_SSL_MODE = "ssl.mode";

    /* Postgres configs */
    public static final String PG_SLOT_NAME = "slot.name";
    public static final String PG_PUB_NAME = "publication.name";
    public static final String PG_PUB_CREATE = "publication.create.enable";
    public static final String PG_SCHEMA_NAME = "schema.name";
    public static final String PG_SSL_ROOT_CERT = "ssl.root.cert";
    public static final String PG_TEST_ONLY_FORCE_RDS = "test.only.force.rds";

    /* Sql Server configs */
    public static final String SQL_SERVER_SCHEMA_NAME = "schema.name";
    public static final String SQL_SERVER_ENCRYPT = "database.encrypt";

    /* RisingWave configs */
    private static final String DBZ_CONFIG_FILE = "debezium.properties";
    private static final String MYSQL_CONFIG_FILE = "mysql.properties";
    private static final String POSTGRES_CONFIG_FILE = "postgres.properties";
    private static final String MONGODB_CONFIG_FILE = "mongodb.properties";
    private static final String SQL_SERVER_CONFIG_FILE = "sql_server.properties";

    private static final String DBZ_PROPERTY_PREFIX = "debezium.";

    private static final String SNAPSHOT_MODE_KEY = "debezium.snapshot.mode";
    private static final String SNAPSHOT_MODE_BACKFILL = "rw_cdc_backfill";

    public static class MongoDb {
        public static final String MONGO_URL = "mongodb.url";
        public static final String MONGO_COLLECTION_NAME = "collection.name";
    }

    private static Map<String, String> extractDebeziumProperties(
            Map<String, String> userProperties) {
        // retain only debezium properties if any
        var dbzProps = new HashMap<String, String>();
        for (var entry : userProperties.entrySet()) {
            var key = entry.getKey();
            if (key.startsWith(DBZ_PROPERTY_PREFIX)) {
                // remove the prefix
                dbzProps.put(key.substring(DBZ_PROPERTY_PREFIX.length()), entry.getValue());
            }
        }
        return dbzProps;
    }

    private final long sourceId;
    private final SourceTypeE sourceType;
    private final Properties resolvedDbzProps;
    private final boolean isBackfillSource;
    private final int waitStreamingStartTimeout;

    public long getSourceId() {
        return sourceId;
    }

    public SourceTypeE getSourceType() {
        return sourceType;
    }

    public Properties getResolvedDebeziumProps() {
        return resolvedDbzProps;
    }

    public boolean isBackfillSource() {
        return isBackfillSource;
    }

    public int getWaitStreamingStartTimeout() {
        return waitStreamingStartTimeout;
    }

    public DbzConnectorConfig(
            SourceTypeE source,
            long sourceId,
            String startOffset,
            Map<String, String> userProps,
            boolean snapshotDone,
            boolean isCdcSourceJob) {

        StringSubstitutor substitutor = new StringSubstitutor(userProps);
        var dbzProps = initiateDbConfig(DBZ_CONFIG_FILE, substitutor);
        var isCdcBackfill =
                null != userProps.get(SNAPSHOT_MODE_KEY)
                        && userProps.get(SNAPSHOT_MODE_KEY).equals(SNAPSHOT_MODE_BACKFILL);
        var waitStreamingStartTimeout =
                Integer.parseInt(
                        userProps.getOrDefault(WAIT_FOR_STREAMING_START_TIMEOUT_SECS, "30"));

        LOG.info(
                "DbzConnectorConfig: source={}, sourceId={}, startOffset={}, snapshotDone={}, isCdcBackfill={}, isCdcSourceJob={}",
                source,
                sourceId,
                startOffset,
                snapshotDone,
                isCdcBackfill,
                isCdcSourceJob);

        if (source == SourceTypeE.MYSQL) {
            var mysqlProps = initiateDbConfig(MYSQL_CONFIG_FILE, substitutor);
            if (isCdcBackfill) {
                // disable snapshot locking at all
                mysqlProps.setProperty("snapshot.locking.mode", "none");

                // If cdc backfill enabled, the source only emit incremental changes, so we must
                // rewind to the given offset and continue binlog reading from there
                if (null != startOffset && !startOffset.isBlank()) {
                    mysqlProps.setProperty("snapshot.mode", "recovery");
                    mysqlProps.setProperty(
                            ConfigurableOffsetBackingStore.OFFSET_STATE_VALUE, startOffset);
                } else {
                    // read upstream table schemas and emit incremental changes only
                    mysqlProps.setProperty("snapshot.mode", "no_data");
                }
            } else {
                // if snapshot phase is finished and offset is specified, we will continue binlog
                // reading from the given offset
                if (snapshotDone && null != startOffset && !startOffset.isBlank()) {
                    // 'snapshot.mode=recovery' must be configured if binlog offset is
                    // specified. It only snapshots the schemas, not the data, and continue binlog
                    // reading from the specified offset
                    mysqlProps.setProperty("snapshot.mode", "recovery");
                    mysqlProps.setProperty(
                            ConfigurableOffsetBackingStore.OFFSET_STATE_VALUE, startOffset);
                }
            }

            dbzProps.putAll(mysqlProps);

            if (isCdcSourceJob) {
                // remove table filtering for the shared MySQL source, since we
                // allow user to ingest tables in different database
                LOG.info("Disable table filtering for the shared MySQL source");
                dbzProps.remove("table.include.list");
            }

        } else if (source == SourceTypeE.POSTGRES) {
            var postgresProps = initiateDbConfig(POSTGRES_CONFIG_FILE, substitutor);

            // disable publication auto creation if needed
            var pubAutoCreate =
                    Boolean.parseBoolean(
                            userProps.getOrDefault(DbzConnectorConfig.PG_PUB_CREATE, "true"));
            if (!pubAutoCreate) {
                postgresProps.setProperty("publication.autocreate.mode", "disabled");
            }
            if (isCdcBackfill) {
                // skip the initial snapshot for cdc backfill
                postgresProps.setProperty("snapshot.mode", "no_data");

                // if startOffset is specified, we should continue
                // reading changes from the given offset
                if (null != startOffset && !startOffset.isBlank()) {
                    postgresProps.setProperty(
                            ConfigurableOffsetBackingStore.OFFSET_STATE_VALUE, startOffset);
                }

            } else {
                // if snapshot phase is finished and offset is specified, we will continue reading
                // changes from the given offset
                if (snapshotDone && null != startOffset && !startOffset.isBlank()) {
                    postgresProps.setProperty("snapshot.mode", "no_data");
                    postgresProps.setProperty(
                            ConfigurableOffsetBackingStore.OFFSET_STATE_VALUE, startOffset);
                }
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

            if (isCdcSourceJob) {
                // remove table filtering for the shared Postgres source, since we
                // allow user to ingest tables in different schemas
                LOG.info("Disable table filtering for the shared Postgres source");
                dbzProps.remove("table.include.list");
            }

            if (userProps.containsKey(PG_SSL_ROOT_CERT)) {
                dbzProps.setProperty("database.sslrootcert", userProps.get(PG_SSL_ROOT_CERT));
            }
        } else if (source == SourceTypeE.CITUS) {
            var postgresProps = initiateDbConfig(POSTGRES_CONFIG_FILE, substitutor);

            // citus needs all_tables publication to capture all shards
            postgresProps.setProperty("publication.autocreate.mode", "all_tables");

            // disable publication auto creation if needed
            var pubAutoCreate =
                    Boolean.parseBoolean(
                            userProps.getOrDefault(DbzConnectorConfig.PG_PUB_CREATE, "true"));

            if (!pubAutoCreate) {
                postgresProps.setProperty("publication.autocreate.mode", "disabled");
            }
            // if snapshot phase is finished and offset is specified, we will continue reading
            // changes from the given offset
            if (snapshotDone && null != startOffset && !startOffset.isBlank()) {
                postgresProps.setProperty("snapshot.mode", "no_data");
                postgresProps.setProperty(
                        ConfigurableOffsetBackingStore.OFFSET_STATE_VALUE, startOffset);
            }
            dbzProps.putAll(postgresProps);
        } else if (source == SourceTypeE.MONGODB) {
            var mongodbProps = initiateDbConfig(MONGODB_CONFIG_FILE, substitutor);

            // if snapshot phase is finished and offset is specified, we will continue reading
            // changes from the given offset
            if (snapshotDone && null != startOffset && !startOffset.isBlank()) {
                mongodbProps.setProperty("snapshot.mode", "no_data");
                mongodbProps.setProperty(
                        ConfigurableOffsetBackingStore.OFFSET_STATE_VALUE, startOffset);
            }

            var mongodbUrl = userProps.get(MongoDb.MONGO_URL);
            var collection = userProps.get(MongoDb.MONGO_COLLECTION_NAME);
            var connectionStr = new ConnectionString(mongodbUrl);
            var connectorName =
                    String.format(
                            "MongoDB_%d:%s:%s", sourceId, connectionStr.getHosts(), collection);
            mongodbProps.setProperty("name", connectorName);

            dbzProps.putAll(mongodbProps);
        } else if (source == SourceTypeE.SQL_SERVER) {
            var sqlServerProps = initiateDbConfig(SQL_SERVER_CONFIG_FILE, substitutor);
            // disable snapshot locking at all
            sqlServerProps.setProperty("snapshot.locking.mode", "none");

            if (isCdcBackfill) {
                // if startOffset is specified, we should continue
                // reading changes from the given offset
                if (null != startOffset && !startOffset.isBlank()) {
                    // skip the initial snapshot for cdc backfill
                    sqlServerProps.setProperty("snapshot.mode", "recovery");
                    sqlServerProps.setProperty(
                            ConfigurableOffsetBackingStore.OFFSET_STATE_VALUE, startOffset);
                } else {
                    sqlServerProps.setProperty("snapshot.mode", "no_data");
                }
            } else {
                // if snapshot phase is finished and offset is specified, we will continue reading
                // changes from the given offset
                if (snapshotDone && null != startOffset && !startOffset.isBlank()) {
                    sqlServerProps.setProperty("snapshot.mode", "recovery");
                    sqlServerProps.setProperty(
                            ConfigurableOffsetBackingStore.OFFSET_STATE_VALUE, startOffset);
                }
            }
            dbzProps.putAll(sqlServerProps);
            if (isCdcSourceJob) {
                // remove table filtering for the shared Sql Server source, since we
                // allow user to ingest tables in different schemas
                LOG.info("Disable table filtering for the shared Sql Server source");
                dbzProps.remove("table.include.list");
            }
        } else {
            throw new RuntimeException("unsupported source type: " + source);
        }

        var otherProps = extractDebeziumProperties(userProps);
        for (var entry : otherProps.entrySet()) {
            dbzProps.putIfAbsent(entry.getKey(), entry.getValue());
        }

        this.sourceId = sourceId;
        this.sourceType = source;
        this.resolvedDbzProps = dbzProps;
        this.isBackfillSource = isCdcBackfill;
        this.waitStreamingStartTimeout = waitStreamingStartTimeout;
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
