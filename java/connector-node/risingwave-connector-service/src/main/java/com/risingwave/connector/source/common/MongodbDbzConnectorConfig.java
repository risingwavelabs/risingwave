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

import com.mongodb.ConnectionString;
import com.risingwave.connector.cdc.debezium.internal.ConfigurableOffsetBackingStore;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongodbDbzConnectorConfig extends DbzConnectorConfig {
    private static final Logger LOG = LoggerFactory.getLogger(MongodbDbzConnectorConfig.class);

    public static final String MONGO_URL = "mongodb.url";
    public static final String COLLECTION_NAME = "collection.name";

    private static final String DATABASE_GROUP = "database";
    private static final Pattern LITERAL_COLLECTION_LIST_PATTERN =
            Pattern.compile(
                    "(?:\\A|[ ]*,[ ]*)"
                            + "(?<database>[A-Za-z_][A-Za-z0-9_-]*)\\."
                            + "(?<collection>[A-Za-z_][A-Za-z0-9_-]*)");
    private static final Set<String> RAW_FILTER_OPTIONS_DISABLING_INFERENCE =
            Set.of(
                    DBZ_PROPERTY_PREFIX + MongoDbConnectorConfig.DATABASE_INCLUDE_LIST.name(),
                    DBZ_PROPERTY_PREFIX + MongoDbConnectorConfig.DATABASE_EXCLUDE_LIST.name(),
                    DBZ_PROPERTY_PREFIX + MongoDbConnectorConfig.FILTERS_MATCH_MODE.name());

    public MongodbDbzConnectorConfig(CdcSource source) {
        super(source);
    }

    @Override
    protected void customizeDebeziumProperties(Properties dbzProps) {
        var mongodbProps = loadPropertiesTemplate("mongodb.properties");

        // if snapshot phase is finished and offset is specified, we will continue reading
        // changes from the given offset
        if (snapshotDone && hasStartOffset()) {
            mongodbProps.setProperty("snapshot.mode", "no_data");
            mongodbProps.setProperty(
                    ConfigurableOffsetBackingStore.OFFSET_STATE_VALUE, startOffset);
        }

        var mongodbUrl = userProps.get(MONGO_URL);
        var collection = userProps.get(COLLECTION_NAME);
        var hasRawFilterOverride =
                RAW_FILTER_OPTIONS_DISABLING_INFERENCE.stream().anyMatch(userProps::containsKey);
        if (!hasRawFilterOverride) {
            inferMongoDatabaseList(collection)
                    .ifPresent(
                            databaseList -> {
                                mongodbProps.setProperty(
                                        MongoDbConnectorConfig.DATABASE_INCLUDE_LIST.name(),
                                        databaseList);
                                mongodbProps.setProperty(
                                        MongoDbConnectorConfig.FILTERS_MATCH_MODE.name(),
                                        MongoDbConnectorConfig.FiltersMatchMode.LITERAL.getValue());
                                LOG.info(
                                        "Inferred MongoDB database include list '{}' and"
                                                + " literal filter match mode from collection list"
                                                + " '{}'",
                                        databaseList,
                                        collection);
                            });
        }

        var connectionStr = new ConnectionString(mongodbUrl);
        var connectorName =
                String.format("MongoDB_%d:%s:%s", sourceId, connectionStr.getHosts(), collection);
        mongodbProps.setProperty("name", connectorName);

        dbzProps.putAll(mongodbProps);
    }

    private static Optional<String> inferMongoDatabaseList(String collectionList) {
        if (collectionList == null) {
            return Optional.empty();
        }

        var matcher = LITERAL_COLLECTION_LIST_PATTERN.matcher(collectionList);
        var databases = new LinkedHashSet<String>();
        var matchedUntil = 0;
        while (matcher.find()) {
            if (matcher.start() != matchedUntil) {
                return Optional.empty();
            }
            databases.add(matcher.group(DATABASE_GROUP));
            matchedUntil = matcher.end();
        }

        if (databases.isEmpty() || matchedUntil != collectionList.length()) {
            return Optional.empty();
        }
        return Optional.of(String.join(",", databases));
    }
}
