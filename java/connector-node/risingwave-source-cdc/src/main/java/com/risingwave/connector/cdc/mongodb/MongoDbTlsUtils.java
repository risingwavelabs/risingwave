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

package com.risingwave.connector.cdc.mongodb;

import com.mongodb.ConnectionString;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

/** Utilities for applying MongoDB TLS URI options that are not handled by the Java driver. */
public final class MongoDbTlsUtils {
    private static final String TLS_CA_FILE = "tlsCAFile";
    private static final String TLS_CERTIFICATE_KEY_FILE = "tlsCertificateKeyFile";
    public static final String TLS_CA_FILE_CONFIG = "risingwave.mongodb.tls.ca.file";
    public static final String TLS_CERTIFICATE_KEY_FILE_CONFIG =
            "risingwave.mongodb.tls.certificate.key.file";

    private MongoDbTlsUtils() {}

    /** Paths to the optional trust and client identity PEM files. */
    public record TlsFiles(Optional<Path> caFile, Optional<Path> certificateKeyFile) {
        public boolean isEmpty() {
            return caFile.isEmpty() && certificateKeyFile.isEmpty();
        }
    }

    /** Returns the custom CA and client certificate paths from a MongoDB connection string. */
    public static TlsFiles tlsFiles(String connectionString) {
        return new TlsFiles(
                tlsFile(connectionString, TLS_CA_FILE),
                tlsFile(connectionString, TLS_CERTIFICATE_KEY_FILE));
    }

    /**
     * Removes TLS PEM file options, which the Java MongoDB driver does not recognize, from a URI.
     */
    public static String withoutTlsFileOptions(String connectionString) {
        int queryStart = connectionString.indexOf('?');
        if (queryStart < 0) {
            return connectionString;
        }

        int fragmentStart = connectionString.indexOf('#', queryStart + 1);
        String fragment = fragmentStart < 0 ? "" : connectionString.substring(fragmentStart);
        List<String> options = new ArrayList<>();
        for (String option : queryOptions(connectionString)) {
            if (!isOption(option, TLS_CA_FILE) && !isOption(option, TLS_CERTIFICATE_KEY_FILE)) {
                options.add(option);
            }
        }

        String prefix = connectionString.substring(0, queryStart);
        return options.isEmpty()
                ? prefix + fragment
                : prefix + "?" + String.join("&", options) + fragment;
    }

    /** Builds an in-memory TLS context from the PEM files referenced by the connection string. */
    public static Optional<SSLContext> createTlsSslContext(String connectionString) {
        TlsFiles tlsFiles = tlsFiles(connectionString);
        if (tlsFiles.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(createTlsSslContext(withoutTlsFileOptions(connectionString), tlsFiles));
    }

    /** Builds an in-memory TLS context from previously extracted PEM file paths. */
    public static SSLContext createTlsSslContext(String connectionString, TlsFiles tlsFiles) {
        ConnectionString parsed = new ConnectionString(connectionString);
        if (Boolean.FALSE.equals(parsed.getSslEnabled())) {
            throw new IllegalArgumentException(
                    "MongoDB TLS certificate file options cannot be set when TLS is disabled");
        }

        try {
            TrustManager[] trustManagers = null;
            if (tlsFiles.caFile().isPresent()) {
                trustManagers = createTrustManagers(tlsFiles.caFile().orElseThrow());
            }

            KeyManager[] keyManagers = null;
            if (tlsFiles.certificateKeyFile().isPresent()) {
                keyManagers = createKeyManagers(tlsFiles.certificateKeyFile().orElseThrow());
            }

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagers, trustManagers, null);
            return sslContext;
        } catch (IOException | GeneralSecurityException e) {
            throw new IllegalArgumentException(
                    "Failed to load MongoDB TLS certificate files: " + tlsFiles, e);
        }
    }

    private static TrustManager[] createTrustManagers(Path caFile)
            throws IOException, GeneralSecurityException {
        List<X509Certificate> certificates = readCertificates(caFile);
        if (certificates.isEmpty()) {
            throw new IllegalArgumentException("No PEM-encoded certificates found in " + caFile);
        }

        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        int index = 0;
        for (Certificate certificate : certificates) {
            trustStore.setCertificateEntry("mongodb-ca-" + index++, certificate);
        }

        TrustManagerFactory trustManagerFactory =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);
        return trustManagerFactory.getTrustManagers();
    }

    private static KeyManager[] createKeyManagers(Path certificateKeyFile)
            throws IOException, GeneralSecurityException {
        String pem = Files.readString(certificateKeyFile, StandardCharsets.US_ASCII);
        List<X509Certificate> certificates = readCertificates(pem, certificateKeyFile);
        if (certificates.isEmpty()) {
            throw new IllegalArgumentException(
                    "No PEM-encoded client certificates found in " + certificateKeyFile);
        }

        PrivateKey privateKey = readPrivateKey(pem, certificates.get(0), certificateKeyFile);
        char[] password = new char[0];
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        keyStore.setKeyEntry(
                "mongodb-client", privateKey, password, certificates.toArray(Certificate[]::new));

        KeyManagerFactory keyManagerFactory =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, password);
        return keyManagerFactory.getKeyManagers();
    }

    private static List<X509Certificate> readCertificates(Path path)
            throws IOException, GeneralSecurityException {
        return readCertificates(Files.readString(path, StandardCharsets.US_ASCII), path);
    }

    private static List<X509Certificate> readCertificates(String pem, Path path)
            throws GeneralSecurityException {
        var matcher = pemPattern("CERTIFICATE").matcher(pem);
        CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
        List<X509Certificate> certificates = new ArrayList<>();
        while (matcher.find()) {
            byte[] der = decodePemBlock(matcher.group(1), "certificate", path);
            try (InputStream input = new ByteArrayInputStream(der)) {
                certificates.add((X509Certificate) certificateFactory.generateCertificate(input));
            } catch (IOException e) {
                throw new GeneralSecurityException(e);
            }
        }
        return certificates;
    }

    private static PrivateKey readPrivateKey(
            String pem, X509Certificate certificate, Path certificateKeyFile)
            throws GeneralSecurityException {
        if (pem.contains("-----BEGIN ENCRYPTED PRIVATE KEY-----")) {
            throw new IllegalArgumentException(
                    "Encrypted MongoDB client private keys are not supported: "
                            + certificateKeyFile);
        }

        byte[] pkcs8 = findPemBlock(pem, "PRIVATE KEY", certificateKeyFile).orElse(null);
        if (pkcs8 == null) {
            byte[] pkcs1 = findPemBlock(pem, "RSA PRIVATE KEY", certificateKeyFile).orElse(null);
            if (pkcs1 != null) {
                pkcs8 = wrapPkcs1RsaPrivateKey(pkcs1);
            }
        }
        if (pkcs8 == null) {
            throw new IllegalArgumentException(
                    "No unencrypted PKCS#8 or RSA private key found in " + certificateKeyFile);
        }

        String algorithm = certificate.getPublicKey().getAlgorithm();
        return KeyFactory.getInstance(algorithm).generatePrivate(new PKCS8EncodedKeySpec(pkcs8));
    }

    private static Optional<Path> tlsFile(String connectionString, String optionName) {
        String value = null;
        for (String option : queryOptions(connectionString)) {
            if (!isOption(option, optionName)) {
                continue;
            }
            if (value != null) {
                throw new IllegalArgumentException(
                        "MongoDB connection option '"
                                + optionName
                                + "' must not be specified more than once");
            }
            int separator = option.indexOf('=');
            value = decode(separator < 0 ? "" : option.substring(separator + 1));
        }

        if (value == null) {
            return Optional.empty();
        }
        if (value.isBlank()) {
            throw new IllegalArgumentException(
                    "MongoDB connection option '" + optionName + "' must not be empty");
        }
        return Optional.of(Path.of(value));
    }

    private static List<String> queryOptions(String connectionString) {
        int queryStart = connectionString.indexOf('?');
        if (queryStart < 0 || queryStart == connectionString.length() - 1) {
            return List.of();
        }

        String query = connectionString.substring(queryStart + 1);
        int fragmentStart = query.indexOf('#');
        if (fragmentStart >= 0) {
            query = query.substring(0, fragmentStart);
        }
        return List.of(query.split("&"));
    }

    private static boolean isOption(String option, String optionName) {
        int separator = option.indexOf('=');
        String key = decode(separator < 0 ? option : option.substring(0, separator));
        return optionName.equalsIgnoreCase(key);
    }

    private static Optional<byte[]> findPemBlock(String pem, String label, Path path)
            throws GeneralSecurityException {
        var matcher = pemPattern(label).matcher(pem);
        if (!matcher.find()) {
            return Optional.empty();
        }
        return Optional.of(decodePemBlock(matcher.group(1), label, path));
    }

    private static Pattern pemPattern(String label) {
        return Pattern.compile(
                "-----BEGIN "
                        + Pattern.quote(label)
                        + "-----\\s*(.*?)\\s*-----END "
                        + Pattern.quote(label)
                        + "-----",
                Pattern.DOTALL);
    }

    private static byte[] decodePemBlock(String value, String label, Path path)
            throws GeneralSecurityException {
        try {
            return Base64.getMimeDecoder().decode(value);
        } catch (IllegalArgumentException e) {
            throw new GeneralSecurityException("Invalid " + label + " in " + path, e);
        }
    }

    private static byte[] wrapPkcs1RsaPrivateKey(byte[] pkcs1) {
        byte[] version = {0x02, 0x01, 0x00};
        byte[] rsaAlgorithmIdentifier = {
            0x30,
            0x0d,
            0x06,
            0x09,
            0x2a,
            (byte) 0x86,
            0x48,
            (byte) 0x86,
            (byte) 0xf7,
            0x0d,
            0x01,
            0x01,
            0x01,
            0x05,
            0x00
        };
        return derValue(0x30, concatenate(version, rsaAlgorithmIdentifier, derValue(0x04, pkcs1)));
    }

    private static byte[] derValue(int tag, byte[] value) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        output.write(tag);
        int length = value.length;
        if (length < 128) {
            output.write(length);
        } else {
            int byteCount = (Integer.SIZE - Integer.numberOfLeadingZeros(length) + 7) / 8;
            output.write(0x80 | byteCount);
            for (int shift = (byteCount - 1) * 8; shift >= 0; shift -= 8) {
                output.write((length >> shift) & 0xff);
            }
        }
        output.writeBytes(value);
        return output.toByteArray();
    }

    private static byte[] concatenate(byte[]... values) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        for (byte[] value : values) {
            output.writeBytes(value);
        }
        return output.toByteArray();
    }

    private static String decode(String value) {
        try {
            return URLDecoder.decode(value, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid percent-encoding in MongoDB URI option", e);
        }
    }
}
