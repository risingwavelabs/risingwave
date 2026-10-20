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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.cert.CertificateFactory;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.Properties;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

/** The SSL contract shared by MySQL validation, streaming, and the Rust clients. */
public final class MySqlSslConfig {
    // Truststores contain public CA certificates only. Reuse them across source validation,
    // restarts, and parallel readers, and keep them available until the JVM exits.
    private static final Map<String, Path> TRUST_STORES = new HashMap<>();
    private static final String TRUST_STORE_PASSWORD = "risingwave";

    private final String mode;
    private final Path trustStore;

    private MySqlSslConfig(String mode, Path trustStore) {
        this.mode = mode;
        this.trustStore = trustStore;
    }

    public static MySqlSslConfig fromProperties(Map<String, String> properties) {
        for (String key : properties.keySet()) {
            if (key.startsWith("database.ssl.") || key.startsWith("debezium.database.ssl.")) {
                throw new IllegalArgumentException(
                        "unsupported MySQL SSL option `"
                                + key
                                + "`; use `ssl.mode` and `ssl.root.cert`");
            }
        }
        String value = option(properties, "ssl.mode", "debezium.database.sslmode");
        String mode =
                switch (value == null ? "disabled" : value) {
                    case "disabled", "disable" -> "disabled";
                    case "required", "require" -> "required";
                    case "verify-ca", "verifyca" -> "verify_ca";
                    case "verify-full", "verifyfull" -> "verify_identity";
                    case "preferred", "prefer" -> "preferred";
                    default ->
                            throw new IllegalArgumentException(
                                    "invalid MySQL ssl.mode; expected disabled, preferred, required, verify-ca, or verify-full");
                };
        String rootCert = option(properties, "ssl.root.cert", "debezium.database.sslrootcert");
        if (rootCert != null) {
            if (rootCert.isEmpty()) {
                throw new IllegalArgumentException("MySQL ssl.root.cert must not be empty");
            }
            if (!mode.equals("verify_ca") && !mode.equals("verify_identity")) {
                // Debezium's binlog client verifies a supplied truststore even in required mode,
                // whereas Connector/J does not. Reject this ambiguous combination everywhere.
                throw new IllegalArgumentException(
                        "MySQL ssl.root.cert requires ssl.mode=verify-ca or verify-full");
            }
        }
        return new MySqlSslConfig(mode, rootCert == null ? null : trustStore(rootCert));
    }

    private static String option(Map<String, String> properties, String name, String alias) {
        if (properties.containsKey(name) && properties.containsKey(alias)) {
            throw new IllegalArgumentException(
                    "conflicting MySQL SSL options `"
                            + name
                            + "` and `"
                            + alias
                            + "`; specify only one");
        }
        return properties.getOrDefault(name, properties.get(alias));
    }

    public void configureJdbc(Properties properties) {
        properties.setProperty("sslMode", mode.toUpperCase(java.util.Locale.ROOT));
        if (trustStore != null) {
            properties.setProperty("trustCertificateKeyStoreUrl", trustStore.toUri().toString());
            properties.setProperty("trustCertificateKeyStoreType", "JKS");
            properties.setProperty("trustCertificateKeyStorePassword", TRUST_STORE_PASSWORD);
            properties.setProperty("fallbackToSystemTrustStore", "false");
        }
    }

    public void configureDebezium(Properties properties) {
        properties.setProperty("database.ssl.mode", mode);
        if (trustStore != null) {
            properties.setProperty("database.ssl.truststore", trustStore.toString());
            properties.setProperty("database.ssl.truststore.password", TRUST_STORE_PASSWORD);
        }
    }

    private static synchronized Path trustStore(String rootCert) {
        Path path = null;
        try {
            // Key by contents so replacing a CA file takes effect on the next connection setup.
            byte[] pem = Files.readAllBytes(Path.of(rootCert));
            String digest =
                    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(pem));
            Path cached = TRUST_STORES.get(digest);
            if (cached != null) {
                return cached;
            }
            if (!new String(pem, StandardCharsets.US_ASCII)
                    .contains("-----BEGIN CERTIFICATE-----")) {
                throw new IllegalArgumentException(
                        "MySQL ssl.root.cert must be a PEM CA certificate");
            }
            var certificates =
                    CertificateFactory.getInstance("X.509")
                            .generateCertificates(new ByteArrayInputStream(pem));
            if (certificates.size() != 1) {
                throw new IllegalArgumentException(
                        "MySQL ssl.root.cert must contain exactly one PEM CA certificate");
            }
            KeyStore store = KeyStore.getInstance("JKS");
            store.load(null, null);
            // SQLx's native TLS backend adds the supplied CA to its default trust roots.
            // Preserve the JVM's default roots too, so validation and streaming agree.
            var trustManagers =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagers.init((KeyStore) null);
            int index = 0;
            for (var manager : trustManagers.getTrustManagers()) {
                if (manager instanceof X509TrustManager x509) {
                    for (var certificate : x509.getAcceptedIssuers()) {
                        store.setCertificateEntry("default-" + index++, certificate);
                    }
                }
            }
            store.setCertificateEntry("ca", certificates.iterator().next());
            path = Files.createTempFile("rw-mysql-ca-", ".jks");
            path.toFile().deleteOnExit();
            try (var output = Files.newOutputStream(path)) {
                store.store(output, TRUST_STORE_PASSWORD.toCharArray());
            }
            TRUST_STORES.put(digest, path);
            return path;
        } catch (IOException | GeneralSecurityException e) {
            if (path != null) {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException cleanupError) {
                    e.addSuppressed(cleanupError);
                }
            }
            throw new IllegalArgumentException("failed to load MySQL ssl.root.cert", e);
        }
    }
}
