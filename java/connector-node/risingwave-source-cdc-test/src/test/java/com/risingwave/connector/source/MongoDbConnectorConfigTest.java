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

package com.risingwave.connector.source;

import static org.assertj.core.api.Assertions.assertThat;

import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.connector.source.common.DbzConnectorConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Test;

public class MongoDbConnectorConfigTest {

    @Test
    public void testExplicitDatabaseListPassesThrough() {
        var properties = resolvedProperties(".*[.]events", Map.of("database.list", "db1,db2"));

        assertThat(properties)
                .containsEntry("database.include.list", "db1,db2")
                .containsEntry("collection.include.list", ".*[.]events");
    }

    @Test
    public void testDatabaseListIsInferredFromCollectionNames() {
        var properties = resolvedProperties("db1.events,db2.events_.*", Map.of());

        assertThat(properties)
                .containsEntry("database.include.list", "db1,db2")
                .containsEntry("collection.include.list", "db1.events,db2.events_.*");
    }

    @Test
    public void testDatabaseListIsNotInferredFromUnqualifiedCollectionName() {
        var properties = resolvedProperties("events", Map.of());

        assertThat(properties)
                .doesNotContainKey("database.include.list")
                .containsEntry("collection.include.list", "events");
    }

    @Test
    public void testDatabaseListIsNotPartiallyInferred() {
        var properties = resolvedProperties("db1.events,.*[.]events", Map.of());

        assertThat(properties).doesNotContainKey("database.include.list");
    }

    @Test
    public void testDatabaseListIsNotInferredFromInvalidDatabaseName() {
        var properties = resolvedProperties("invalid name.events", Map.of());

        assertThat(properties).doesNotContainKey("database.include.list");
    }

    @Test
    public void testLegacyDebeziumDatabaseListPassesThrough() {
        var properties =
                resolvedProperties(
                        "db1.events", Map.of("debezium.database.include.list", "legacy_db"));

        assertThat(properties)
                .containsEntry("database.include.list", "legacy_db")
                .containsEntry("collection.include.list", "db1.events");
    }

    @Test
    public void testLegacyDebeziumDatabaseListTakesPrecedenceOverDatabaseList() {
        var properties =
                resolvedProperties(
                        "db1.events",
                        Map.of(
                                "database.list", "db1",
                                "debezium.database.include.list", "legacy_db"));

        assertThat(properties).containsEntry("database.include.list", "legacy_db");
    }

    @Test
    public void testLegacyDebeziumDatabaseExcludeListPreventsInference() {
        var properties =
                resolvedProperties(
                        "db1.events", Map.of("debezium.database.exclude.list", "legacy_db"));

        assertThat(properties)
                .doesNotContainKey("database.include.list")
                .containsEntry("database.exclude.list", "legacy_db");
    }

    @Test
    public void testCollectionNameTakesPrecedenceOverDebeziumOption() {
        var properties =
                resolvedProperties(
                        "db1.events", Map.of("debezium.collection.include.list", "db2.events"));

        assertThat(properties).containsEntry("collection.include.list", "db1.events");
    }

    private static Map<String, String> resolvedProperties(
            String collectionName, Map<String, String> additionalProperties) {
        var userProperties = new HashMap<String, String>();
        userProperties.put("mongodb.url", "mongodb://localhost:27017/?replicaSet=rs0");
        userProperties.put("collection.name", collectionName);
        userProperties.putAll(additionalProperties);

        var config =
                new DbzConnectorConfig(SourceTypeE.MONGODB, 1, null, userProperties, false, false);
        return config.getResolvedDebeziumProps().entrySet().stream()
                .collect(
                        Collectors.toMap(
                                entry -> entry.getKey().toString(),
                                entry -> entry.getValue().toString()));
    }
}
