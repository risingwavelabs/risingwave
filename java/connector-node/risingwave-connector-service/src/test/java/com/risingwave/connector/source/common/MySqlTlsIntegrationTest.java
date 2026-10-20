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
import static org.junit.Assume.assumeTrue;

import com.risingwave.connector.api.source.SourceTypeE;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.jdbc.MySqlConnection;
import io.debezium.connector.mysql.jdbc.MySqlConnectionConfiguration;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

/** Run with MYSQL_TLS_HOST, MYSQL_TLS_WRONG_HOST, MYSQL_TLS_PORT, and MYSQL_TLS_CERT_DIR. */
public class MySqlTlsIntegrationTest {
    @Test
    public void validatesJdbcAndDebeziumConnections() throws Exception {
        String host = System.getenv("MYSQL_TLS_HOST");
        assumeTrue("requires the ci/mysql-tls fixture", host != null);
        String wrongHost = System.getenv("MYSQL_TLS_WRONG_HOST");
        String certDir = System.getenv("MYSQL_TLS_CERT_DIR");
        for (String[] test :
                new String[][] {
                    {"disabled", host, null, "false"},
                    {"required", host, null, "true"},
                    {"verify-ca", host, "ca.pem", "true"},
                    {"verify-ca", wrongHost, "ca.pem", "true"},
                    {"verify-full", host, "ca.pem", "true"},
                    {"verify-full", wrongHost, "ca.pem", "false"},
                    {"verify-ca", host, "untrusted-ca.pem", "false"},
                    {"verify-full", host, "untrusted-ca.pem", "false"},
                }) {
            var options =
                    new HashMap<>(
                            Map.of(
                                    "ssl.mode",
                                    test[0],
                                    "hostname",
                                    test[1],
                                    "port",
                                    System.getenv("MYSQL_TLS_PORT"),
                                    "username",
                                    "root",
                                    "password",
                                    "123456",
                                    "database.name",
                                    "mysql"));
            if (test[2] != null) {
                options.put("ssl.root.cert", certDir + "/" + test[2]);
            }
            boolean succeeds = Boolean.parseBoolean(test[3]);
            String description = java.util.Arrays.toString(test);
            try (var validator = new MySqlValidator(options, null, true, false)) {
                assertTrue(description, succeeds);
            } catch (Exception e) {
                if (succeeds) {
                    throw new AssertionError(description, e);
                }
            }
            var dbz =
                    new DbzConnectorConfig(SourceTypeE.MYSQL, 42, null, options, false, true)
                            .getResolvedDebeziumProps();
            try (var connection =
                    new MySqlConnection(
                            new MySqlConnectionConfiguration(Configuration.from(dbz)), null)) {
                connection.query(
                        "SHOW SESSION STATUS LIKE 'Ssl_cipher'",
                        rows -> {
                            assertTrue(rows.next());
                            assertFalse(rows.getString(2).isEmpty());
                        });
                assertTrue(description, succeeds);
            } catch (Exception e) {
                if (succeeds) {
                    throw new AssertionError(description, e);
                }
            }
        }
    }
}
