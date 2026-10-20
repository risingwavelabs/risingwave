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
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.Test;

public class MySqlSslConfigTest {
    @Test
    public void translatesModesForValidationAndStreamingIncludingResume() {
        for (var entry :
                Map.of(
                                "disabled",
                                "disabled",
                                "disable",
                                "disabled",
                                "required",
                                "required",
                                "require",
                                "required",
                                "verify-ca",
                                "verify_ca",
                                "verifyca",
                                "verify_ca",
                                "verify-full",
                                "verify_identity",
                                "verifyfull",
                                "verify_identity")
                        .entrySet()) {
            for (String key : new String[] {"ssl.mode", "debezium.database.sslmode"}) {
                var userProps = new HashMap<>(Map.of(key, entry.getKey()));
                var jdbc = new Properties();
                MySqlSslConfig.fromProperties(userProps).configureJdbc(jdbc);
                assertEquals(
                        entry.getValue().toUpperCase(java.util.Locale.ROOT),
                        jdbc.getProperty("sslMode"));
                for (String offset : new String[] {null, "saved-offset"}) {
                    for (boolean backfill : new boolean[] {false, true}) {
                        userProps.put(
                                "debezium.snapshot.mode", backfill ? "rw_cdc_backfill" : "initial");
                        var config =
                                new DbzConnectorConfig(
                                        SourceTypeE.MYSQL, 42, offset, userProps, true, false);
                        assertEquals(
                                entry.getValue(),
                                config.getResolvedDebeziumProps().getProperty("database.ssl.mode"));
                    }
                }
            }
        }
        var jdbc = new Properties();
        MySqlSslConfig.fromProperties(Map.of()).configureJdbc(jdbc);
        assertEquals("DISABLED", jdbc.getProperty("sslMode"));
    }

    @Test
    public void rejectsAmbiguousOrUnsupportedOptions() {
        for (String mode :
                new String[] {
                    "preferred", "prefer", "verify_ca", "verify_identity", "REQUIRED", "bogus"
                }) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> MySqlSslConfig.fromProperties(Map.of("ssl.mode", mode)));
        }
        for (String key :
                new String[] {
                    "database.ssl.mode",
                    "debezium.database.ssl.mode",
                    "debezium.database.ssl.truststore"
                }) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> MySqlSslConfig.fromProperties(Map.of(key, "required")));
        }
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        MySqlSslConfig.fromProperties(
                                Map.of(
                                        "ssl.mode",
                                        "required",
                                        "debezium.database.sslmode",
                                        "required")));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        MySqlSslConfig.fromProperties(
                                Map.of(
                                        "ssl.root.cert",
                                        "/ca.pem",
                                        "debezium.database.sslrootcert",
                                        "/ca.pem")));
        for (String mode : new String[] {"disabled", "required"}) {
            assertThrows(
                    IllegalArgumentException.class,
                    () ->
                            MySqlSslConfig.fromProperties(
                                    Map.of("ssl.mode", mode, "ssl.root.cert", "/ca.pem")));
        }
    }

    @Test
    public void sharesPemTrustAcrossJdbcAndDebeziumAndRejectsBundles() throws Exception {
        var path = Files.createTempFile("mysql-test-ca-", ".pem");
        try {
            byte[] pem;
            try (var input = getClass().getResourceAsStream("/mysql-tls/ca.pem")) {
                assertNotNull(input);
                pem = input.readAllBytes();
            }
            Files.write(path, pem);
            var options =
                    Map.of(
                            "ssl.mode",
                            "verify-full",
                            "debezium.database.sslrootcert",
                            path.toString());
            var jdbc = new Properties();
            MySqlSslConfig.fromProperties(options).configureJdbc(jdbc);
            var dbz =
                    new DbzConnectorConfig(
                                    SourceTypeE.MYSQL, 42, "saved-offset", options, true, false)
                            .getResolvedDebeziumProps();
            var storePath = java.nio.file.Path.of(dbz.getProperty("database.ssl.truststore"));
            assertEquals(
                    storePath.toUri().toString(), jdbc.getProperty("trustCertificateKeyStoreUrl"));
            assertEquals("false", jdbc.getProperty("fallbackToSystemTrustStore"));
            var store = java.security.KeyStore.getInstance("JKS");
            try (var input = Files.newInputStream(storePath)) {
                store.load(
                        input, jdbc.getProperty("trustCertificateKeyStorePassword").toCharArray());
            }
            assertTrue(store.size() > 1);
            var certificate =
                    java.security.cert.CertificateFactory.getInstance("X.509")
                            .generateCertificate(new java.io.ByteArrayInputStream(pem));
            assertEquals(certificate, store.getCertificate("ca"));
            // SQLx native TLS loads a single certificate. Reject bundles in source validation.
            Files.write(path, pem, java.nio.file.StandardOpenOption.APPEND);
            assertThrows(
                    IllegalArgumentException.class, () -> MySqlSslConfig.fromProperties(options));
        } finally {
            Files.deleteIfExists(path);
        }
    }

    @Test
    public void rejectsMissingEmptyAndInvalidCertificates() throws Exception {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        MySqlSslConfig.fromProperties(
                                Map.of("ssl.mode", "verify-ca", "ssl.root.cert", "")));
        var path = Files.createTempFile("mysql-invalid-ca-", ".pem");
        try {
            for (String contents : new String[] {"", "not a certificate"}) {
                Files.writeString(path, contents);
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MySqlSslConfig.fromProperties(
                                        Map.of(
                                                "ssl.mode",
                                                "verify-ca",
                                                "ssl.root.cert",
                                                path.toString())));
            }
        } finally {
            Files.delete(path);
        }
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        MySqlSslConfig.fromProperties(
                                Map.of("ssl.mode", "verify-ca", "ssl.root.cert", path.toString())));
    }
}
