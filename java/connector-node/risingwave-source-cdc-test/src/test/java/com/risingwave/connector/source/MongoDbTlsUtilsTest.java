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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.connector.cdc.mongodb.MongoDbTlsUtils;
import com.risingwave.connector.source.common.DbzConnectorConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class MongoDbTlsUtilsTest {
    private static final String TEST_CA =
            """
            -----BEGIN CERTIFICATE-----
            MIIDKzCCAhOgAwIBAgIUasoPCQmxVsB1pHcu26t1oFXqPwMwDQYJKoZIhvcNAQEL
            BQAwJTEjMCEGA1UEAwwaUmlzaW5nV2F2ZSBNb25nb0RCIFRlc3QgQ0EwHhcNMjYw
            ODE4MTAxMTIzWhcNMjYwODE5MTAxMTIzWjAlMSMwIQYDVQQDDBpSaXNpbmdXYXZl
            IE1vbmdvREIgVGVzdCBDQTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB
            AK4WHBdiDKGN+g9VNPT+Q6saCT2273vsBU9y55JybUQac5Yk6rRJMFgBL5L6hr18
            QuYUgTtr7PhCvuzHhnMkIiLxYMKOdwoIPqmi7tvPDhJPcQZV4DOL2HreqQkMAA6o
            6TMUSnF8CBsvFr0mmiJ6IVyt/4wD+ec+7XczaPA+FDzmnj5wLdZ1/OkYxNiVri1g
            NnG41U8iKvpDp1QzV8fHgko3+qXgXvuFG+kLa4HoPGauuOdfUXMpnEHEfBYfljzM
            ayMOTtiYlbMLjvo9CFhegpTB/rtENVUYr8hx+CsJ1Z/iOOJrtWL0OPnz4nzCbpC7
            OMXudPuJM5qYWGaMV8fxz1cCAwEAAaNTMFEwHQYDVR0OBBYEFNg2m6oK6npxoFd7
            5sHMQpdApv1NMB8GA1UdIwQYMBaAFNg2m6oK6npxoFd75sHMQpdApv1NMA8GA1Ud
            EwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBABwehLFu4Che+Rh0gLuSQg/W
            0o0wQuf6li8jgsAHn9sBUVnl/hS86GhvmXQoBD3UWELclFqCAwPtASXj+3182BEa
            Pd4AG+GHOPF0DDkPJgBalcLgoNHAaDqrTeThaYLAU9vwQGYHPVv19LzEKsPdLIJt
            hJiSERZb9FtZ4pLU8jUkve8WXhyXYgQWnm2uC9f23+OcNLk+ebHPUs0I+/AMCPL5
            h2ep7a9OdjDVWYY1a9dwW279FUBZQUgeZy6fk8LQEOjsY9bmX63ZEV2GESbfoZaQ
            JoXqq9NyqHZofzVRtqCKMu8Kvmcj3CTLib1Sd9azOhNPTOahXfpbwiiftwZIO2I=
            -----END CERTIFICATE-----
            """;

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void extractsAndDecodesTlsFileOptions() {
        String connectionString =
                "mongodb://localhost/?tls=true&tlsCAFile=%2Fetc%2Fmongo+certs%2Fca.pem"
                        + "&tlsCertificateKeyFile=%2Fetc%2Fmongo+certs%2Fclient.pem&replicaSet=rs0";
        var tlsFiles = MongoDbTlsUtils.tlsFiles(connectionString);
        assertEquals(Path.of("/etc/mongo certs/ca.pem"), tlsFiles.caFile().orElseThrow());
        assertEquals(
                Path.of("/etc/mongo certs/client.pem"),
                tlsFiles.certificateKeyFile().orElseThrow());
        assertEquals(
                "mongodb://localhost/?tls=true&replicaSet=rs0",
                MongoDbTlsUtils.withoutTlsFileOptions(connectionString));
    }

    @Test
    public void handlesCaseInsensitiveTlsFileOptionsAndFragments() {
        String connectionString =
                "mongodb://localhost/?TLSCAFILE=%2Fetc%2Fmongo%2Fca.pem"
                        + "&appName=risingwave&TlsCertificateKeyFile=%2Fetc%2Fmongo%2Fclient.pem"
                        + "#fragment";

        var tlsFiles = MongoDbTlsUtils.tlsFiles(connectionString);
        assertEquals(Path.of("/etc/mongo/ca.pem"), tlsFiles.caFile().orElseThrow());
        assertEquals(Path.of("/etc/mongo/client.pem"), tlsFiles.certificateKeyFile().orElseThrow());
        assertEquals(
                "mongodb://localhost/?appName=risingwave#fragment",
                MongoDbTlsUtils.withoutTlsFileOptions(connectionString));
    }

    @Test
    public void rejectsDuplicateTlsFileOptions() {
        var error =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MongoDbTlsUtils.tlsFiles(
                                        "mongodb://localhost/?tlsCAFile=%2Fca.pem"
                                                + "&TLSCAFILE=%2Fother-ca.pem"));
        assertTrue(error.getMessage().contains("must not be specified more than once"));
    }

    @Test
    public void rejectsEmptyTlsFileOptions() {
        var error =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MongoDbTlsUtils.tlsFiles(
                                        "mongodb://localhost/?tlsCertificateKeyFile="));
        assertTrue(error.getMessage().contains("must not be empty"));
    }

    @Test
    public void rejectsInvalidPercentEncoding() {
        var error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> MongoDbTlsUtils.tlsFiles("mongodb://localhost/?tlsCAFile=%ZZ"));
        assertTrue(error.getMessage().contains("Invalid percent-encoding"));
    }

    @Test
    public void createsSslContextFromPem() throws Exception {
        Path caFile = temporaryFolder.newFile("ca.pem").toPath();
        Files.writeString(caFile, TEST_CA);

        var sslContext =
                MongoDbTlsUtils.createTlsSslContext("mongodb://localhost/?tlsCAFile=" + caFile)
                        .orElseThrow();

        assertNotNull(sslContext.getSocketFactory());
        assertTrue(sslContext.getProtocol().startsWith("TLS"));
    }

    @Test
    public void rejectsTlsCaFileWhenTlsIsDisabled() throws Exception {
        Path caFile = temporaryFolder.newFile("ca.pem").toPath();
        Files.writeString(caFile, TEST_CA);

        var error =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MongoDbTlsUtils.createTlsSslContext(
                                        "mongodb://localhost/?tls=false&tlsCAFile=" + caFile));
        assertTrue(error.getMessage().contains("TLS is disabled"));
    }

    @Test
    public void rejectsClientCertificatePemWithoutPrivateKey() throws Exception {
        Path certificateKeyFile = temporaryFolder.newFile("client.pem").toPath();
        Files.writeString(certificateKeyFile, TEST_CA);

        var error =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MongoDbTlsUtils.createTlsSslContext(
                                        "mongodb://localhost/?tlsCertificateKeyFile="
                                                + certificateKeyFile));
        assertTrue(error.getMessage().contains("private key"));
    }

    @Test
    public void passesTlsFilePathsSeparatelyToDebezium() {
        var userProps = new HashMap<String, String>();
        userProps.put(
                "mongodb.url",
                "mongodb://localhost/?replicaSet=rs0&tlsCAFile=%2Fetc%2Fmongo%2Fca.pem"
                        + "&tlsCertificateKeyFile=%2Fetc%2Fmongo%2Fclient.pem");
        userProps.put("collection.name", "test.users");

        var config = new DbzConnectorConfig(SourceTypeE.MONGODB, 1, null, userProps, false, false);
        var properties = config.getResolvedDebeziumProps();

        assertEquals(
                "mongodb://localhost/?replicaSet=rs0",
                properties.getProperty("mongodb.connection.string"));
        assertEquals(
                "/etc/mongo/ca.pem", properties.getProperty(MongoDbTlsUtils.TLS_CA_FILE_CONFIG));
        assertEquals(
                "/etc/mongo/client.pem",
                properties.getProperty(MongoDbTlsUtils.TLS_CERTIFICATE_KEY_FILE_CONFIG));
    }
}
