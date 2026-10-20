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

import static org.junit.Assert.*;

import com.risingwave.connector.api.source.SourceTypeE;
import java.util.Map;
import java.util.Properties;
import org.junit.Test;

public class MySqlSslConfigTest {
    @Test
    public void translatesModesForValidationAndStreaming() {
        for (String[] entry :
                new String[][] {
                    {"disabled", "disabled"},
                    {"disable", "disabled"},
                    {"preferred", "preferred"},
                    {"prefer", "preferred"},
                    {"required", "required"},
                    {"require", "required"},
                    {"REQUIRED", "required"},
                    {"verify-ca", "verify_ca"},
                    {"verifyca", "verify_ca"},
                    {"VERIFY_CA", "verify_ca"},
                    {"verify-full", "verify_identity"},
                    {"verifyfull", "verify_identity"},
                    {"VERIFY_IDENTITY", "verify_identity"},
                }) {
            for (String key : new String[] {"ssl.mode", "debezium.database.sslmode"}) {
                var userProps = Map.of(key, entry[0]);
                var jdbc = new Properties();
                MySqlSslConfig.fromProperties(userProps).configureJdbc(jdbc);
                assertEquals(
                        entry[1].toUpperCase(java.util.Locale.ROOT), jdbc.getProperty("sslMode"));
                var config =
                        new DbzConnectorConfig(SourceTypeE.MYSQL, 42, null, userProps, true, false);
                assertEquals(
                        entry[1],
                        config.getResolvedDebeziumProps().getProperty("database.ssl.mode"));
            }
        }
        var jdbc = new Properties();
        MySqlSslConfig.fromProperties(Map.of()).configureJdbc(jdbc);
        assertEquals("DISABLED", jdbc.getProperty("sslMode"));
        assertThrows(
                IllegalArgumentException.class,
                () -> MySqlSslConfig.fromProperties(Map.of("ssl.mode", "bogus")));
    }

    @Test
    public void preservesCanonicalPrecedenceAndDebeziumPassThrough() {
        var options =
                Map.of(
                        "ssl.mode",
                        "required",
                        "debezium.database.sslmode",
                        "bogus",
                        "database.ssl.mode",
                        "disabled",
                        "debezium.database.ssl.mode",
                        "disabled",
                        "debezium.database.ssl.truststore",
                        "/legacy/truststore.jks",
                        "debezium.database.ssl.truststore.password",
                        "legacy-password",
                        "debezium.database.ssl.keystore",
                        "/legacy/keystore.jks");
        var jdbc = new Properties();
        MySqlSslConfig.fromProperties(options).configureJdbc(jdbc);
        assertEquals("REQUIRED", jdbc.getProperty("sslMode"));
        assertEquals(1, jdbc.size());
        var dbz =
                new DbzConnectorConfig(SourceTypeE.MYSQL, 42, null, options, false, true)
                        .getResolvedDebeziumProps();
        assertEquals("required", dbz.getProperty("database.ssl.mode"));
        assertEquals("/legacy/truststore.jks", dbz.getProperty("database.ssl.truststore"));
        assertEquals("legacy-password", dbz.getProperty("database.ssl.truststore.password"));
        assertEquals("/legacy/keystore.jks", dbz.getProperty("database.ssl.keystore"));
        // Generated defaults also continue to win over raw pass-through mode overrides.
        dbz =
                new DbzConnectorConfig(
                                SourceTypeE.MYSQL,
                                42,
                                null,
                                Map.of(
                                        "database.ssl.mode",
                                        "required",
                                        "debezium.database.ssl.mode",
                                        "required"),
                                false,
                                true)
                        .getResolvedDebeziumProps();
        assertEquals("disabled", dbz.getProperty("database.ssl.mode"));
    }

    @Test
    public void ignoresRootCertOptionsForEveryMode() {
        for (String mode :
                new String[] {"disabled", "preferred", "required", "verify-ca", "verify-full"}) {
            var options =
                    Map.of(
                            "ssl.mode",
                            mode,
                            "ssl.root.cert",
                            "",
                            "debezium.database.sslrootcert",
                            "/nonexistent/mysql-ca.pem");
            var jdbc = new Properties();
            MySqlSslConfig.fromProperties(options).configureJdbc(jdbc);
            assertEquals(1, jdbc.size());
            var dbz =
                    new DbzConnectorConfig(SourceTypeE.MYSQL, 42, null, options, false, true)
                            .getResolvedDebeziumProps();
            assertNull(dbz.getProperty("database.ssl.truststore"));
            assertNull(dbz.getProperty("database.ssl.truststore.password"));
        }
    }
}
