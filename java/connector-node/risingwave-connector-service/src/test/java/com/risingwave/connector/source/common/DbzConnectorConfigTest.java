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

import static org.junit.Assert.assertEquals;

import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.connector.cdc.debezium.internal.OpendalSchemaHistory;
import java.util.HashMap;
import org.junit.Test;

public class DbzConnectorConfigTest {
    @Test
    public void usesConstructorSourceIdForTopicPrefix() {
        var userProps = new HashMap<String, String>();
        userProps.put("mongodb.url", "mongodb://localhost:27017");
        userProps.put("collection.name", "test.users");
        userProps.put("source.id", "user-supplied-value");

        var config = new DbzConnectorConfig(SourceTypeE.MONGODB, 42, null, userProps, false, false);

        assertEquals("RW_CDC_42", config.getResolvedDebeziumProps().getProperty("topic.prefix"));
    }

    @Test
    public void configuresDedicatedMariaDbConnectorAndSslMode() {
        var userProps = new HashMap<String, String>();
        userProps.put("hostname", "localhost");
        userProps.put("port", "3306");
        userProps.put("username", "root");
        userProps.put("password", "secret");
        userProps.put("database.name", "test");
        userProps.put("table.name", "orders");
        userProps.put("server.id", "4102");
        userProps.put("ssl.mode", "required");

        var config = new DbzConnectorConfig(SourceTypeE.MARIADB, 43, null, userProps, false, false);

        assertEquals(
                "io.debezium.connector.mariadb.MariaDbConnector",
                config.getResolvedDebeziumProps().getProperty("connector.class"));
        assertEquals("trust", config.getResolvedDebeziumProps().getProperty("database.ssl.mode"));
        assertEquals(
                OpendalSchemaHistory.class.getName(),
                config.getResolvedDebeziumProps().getProperty("schema.history.internal"));
    }
}
