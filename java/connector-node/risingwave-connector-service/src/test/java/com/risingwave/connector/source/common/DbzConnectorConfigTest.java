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
import static org.junit.Assert.assertThrows;

import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.connector.cdc.debezium.internal.OpendalSchemaHistory;
import io.debezium.connector.mariadb.MariaDbStreamingChangeEventSource;
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

    @Test
    public void resolvesMariaDbSslModeAlias() {
        var userProps = new HashMap<String, String>();
        for (var mode : new String[] {"disabled", "required", "verify-ca", "verify-full"}) {
            userProps.put("debezium.database.sslmode", mode);
            var config =
                    new DbzConnectorConfig(SourceTypeE.MARIADB, 43, null, userProps, false, false);
            assertEquals(
                    DbzConnectorConfig.normalizeMariaDbSslMode(mode),
                    config.getResolvedDebeziumProps().getProperty("database.ssl.mode"));
        }
    }

    @Test
    public void prefersCanonicalMariaDbSslModeAndDefaultsToDisabled() {
        var userProps = new HashMap<String, String>();
        var config = new DbzConnectorConfig(SourceTypeE.MARIADB, 43, null, userProps, false, false);
        assertEquals("disable", config.getResolvedDebeziumProps().getProperty("database.ssl.mode"));

        userProps.put("debezium.database.sslmode", "disabled");
        userProps.put("ssl.mode", "verify-full");
        config = new DbzConnectorConfig(SourceTypeE.MARIADB, 43, null, userProps, false, false);
        assertEquals(
                "verify-full", config.getResolvedDebeziumProps().getProperty("database.ssl.mode"));
    }

    @Test
    public void rejectsUnsupportedMariaDbPreferredSslModeAlias() {
        var userProps = new HashMap<String, String>();
        userProps.put("debezium.database.sslmode", "preferred");
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new DbzConnectorConfig(
                                SourceTypeE.MARIADB, 43, null, userProps, false, false));
    }

    @Test
    public void rejectsUnsupportedMariaDbPreferredSslMode() {
        var exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> DbzConnectorConfig.normalizeMariaDbSslMode("preferred"));

        assertEquals(
                "MariaDB CDC does not support ssl.mode 'preferred'; use 'required', 'verify-ca', or"
                        + " 'verify-full'",
                exception.getMessage());
    }

    @Test
    public void forwardsMariaDbRootCertificateAndLegacyAlias() {
        var userProps = new HashMap<String, String>();
        userProps.put("ssl.mode", "verify-full");
        userProps.put("debezium.database.sslrootcert", "/etc/mariadb/legacy-ca.pem");
        var config = new DbzConnectorConfig(SourceTypeE.MARIADB, 44, null, userProps, false, false);
        assertEquals(
                "/etc/mariadb/legacy-ca.pem",
                config.getResolvedDebeziumProps().getProperty("driver.serverSslCert"));

        userProps.put("ssl.root.cert", "/etc/mariadb/ca.pem");
        config = new DbzConnectorConfig(SourceTypeE.MARIADB, 44, null, userProps, false, false);
        assertEquals(
                "/etc/mariadb/ca.pem",
                config.getResolvedDebeziumProps().getProperty("driver.serverSslCert"));
        assertEquals(
                "/etc/mariadb/ca.pem",
                config.getResolvedDebeziumProps()
                        .getProperty(MariaDbStreamingChangeEventSource.SSL_ROOT_CERT_CONFIG));
    }
}
