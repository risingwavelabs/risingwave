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
import static org.junit.Assert.assertThrows;

import com.risingwave.connector.ConnectorServiceImpl;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.connector.source.common.MySqlValidator;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.ConnectorServiceProto.CdcMessage;
import com.risingwave.proto.ConnectorServiceProto.GetEventStreamResponse;
import com.risingwave.proto.ConnectorServiceProto.SourceType;
import com.risingwave.proto.Data;
import com.risingwave.proto.PlanCommon;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ServerBuilder;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.utility.MountableFile;

public class MariaDBTlsSourceTest {
    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void validatesSnapshotsAndStreamsWithPrivateCa() throws Exception {
        Path directory = temporaryFolder.getRoot().toPath();
        var generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        var ca = generator.generateKeyPair();
        var caName = new X500Name("CN=RisingWave MariaDB test CA");
        writeCertificate(directory.resolve("ca.pem"), ca, caName, caName, ca.getPrivate(), true);
        var serverKey = generator.generateKeyPair();
        writeCertificate(
                directory.resolve("server.pem"),
                serverKey,
                new X500Name("CN=localhost"),
                caName,
                ca.getPrivate(),
                false);
        writePem(directory.resolve("server-key.pem"), serverKey.getPrivate());
        var wrongCa = generator.generateKeyPair();
        var wrongName = new X500Name("CN=Untrusted MariaDB test CA");
        writeCertificate(
                directory.resolve("wrong-ca.pem"),
                wrongCa,
                wrongName,
                wrongName,
                wrongCa.getPrivate(),
                true);

        com.risingwave.java.binding.Binding.initObjectStoreForTest(
                "hummock+memory", "mariadb-tls-integration-test-data");
        var server =
                ServerBuilder.forPort(0).addService(new ConnectorServiceImpl()).build().start();
        try (var database =
                new MariaDBContainer<>("mariadb:11.4")
                        .withDatabaseName("test")
                        .withUsername("root")
                        .withPassword("test")
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(directory.resolve("ca.pem")),
                                "/tls/ca.pem")
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(directory.resolve("server.pem")),
                                "/tls/server.pem")
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(directory.resolve("server-key.pem")),
                                "/tls/server-key.pem")
                        .withCommand(
                                "--log-bin=mariadb-bin",
                                "--binlog-format=ROW",
                                "--binlog-row-image=FULL",
                                "--server-id=4301",
                                "--binlog-legacy-event-pos=ON",
                                "--ssl-ca=/tls/ca.pem",
                                "--ssl-cert=/tls/server.pem",
                                "--ssl-key=/tls/server-key.pem")) {
            database.start();
            try (var connection =
                            DriverManager.getConnection(database.getJdbcUrl(), "root", "test");
                    var statement = connection.createStatement()) {
                // Require encryption for both JDBC and replication connections.
                statement.execute("CREATE USER 'cdc'@'%' IDENTIFIED BY 'test' REQUIRE SSL");
                statement.execute("GRANT ALL PRIVILEGES ON *.* TO 'cdc'@'%'");
                for (var mode : new String[] {"verify-ca", "verify-full"}) {
                    String table = mode.equals("verify-ca") ? "orders_ca" : "orders_full";
                    statement.execute("CREATE TABLE " + table + " (id INT PRIMARY KEY)");
                    statement.execute("INSERT INTO " + table + " VALUES (1)");
                    var properties = new HashMap<String, String>();
                    properties.put("hostname", database.getHost());
                    properties.put("port", database.getMappedPort(3306).toString());
                    properties.put("username", "cdc");
                    properties.put("password", "test");
                    properties.put("database.name", "test");
                    properties.put("table.name", table);
                    properties.put("server.id", mode.equals("verify-ca") ? "4302" : "4303");
                    // Exercise the canonical SSL mode and its alias through validation and CDC.
                    properties.put(
                            mode.equals("verify-ca") ? "ssl.mode" : "debezium.database.sslmode",
                            mode);
                    properties.put("debezium.poll.interval.ms", "100");
                    // Exercise both the canonical option and the alias accepted by Rust.
                    String certOption =
                            mode.equals("verify-ca")
                                    ? "ssl.root.cert"
                                    : "debezium.database.sslrootcert";
                    properties.put(certOption, directory.resolve("wrong-ca.pem").toString());
                    var schema =
                            TableSchema.fromProto(
                                    ConnectorServiceProto.TableSchema.newBuilder()
                                            .addColumns(
                                                    PlanCommon.ColumnDesc.newBuilder()
                                                            .setName("id")
                                                            .setColumnType(
                                                                    Data.DataType.newBuilder()
                                                                            .setTypeName(
                                                                                    Data.DataType
                                                                                            .TypeName
                                                                                            .INT32)))
                                            .addPkIndices(0)
                                            .build());
                    assertThrows(
                            SQLException.class,
                            () -> {
                                try (var validator =
                                        new MySqlValidator(
                                                SourceTypeE.MARIADB,
                                                properties,
                                                schema,
                                                false,
                                                false)) {
                                    validator.validateAll();
                                }
                            });
                    properties.put(certOption, directory.resolve("ca.pem").toString());
                    try (var validator =
                            new MySqlValidator(
                                    SourceTypeE.MARIADB, properties, schema, false, false)) {
                        validator.validateAll();
                    }
                    var channel =
                            Grpc.newChannelBuilder(
                                            "localhost:" + server.getPort(),
                                            InsecureChannelCredentials.create())
                                    .build();
                    var reader = Executors.newSingleThreadExecutor();
                    try {
                        var stream =
                                new SourceTestClient(channel)
                                        .getEventStream(
                                                SourceType.MARIADB,
                                                mode.equals("verify-ca") ? 1006 : 1007,
                                                properties);
                        var snapshot =
                                reader.submit(() -> readData(stream)).get(30, TimeUnit.SECONDS);
                        assertThat(snapshot.getPayload()).contains("\"id\":1", "\"op\":\"r\"");
                        statement.execute("INSERT INTO " + table + " VALUES (2)");
                        var insert =
                                reader.submit(() -> readData(stream)).get(30, TimeUnit.SECONDS);
                        assertThat(insert.getPayload()).contains("\"id\":2", "\"op\":\"c\"");
                    } finally {
                        channel.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
                        reader.shutdownNow();
                    }
                }
            }
        } finally {
            server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private static CdcMessage readData(Iterator<GetEventStreamResponse> stream) {
        while (stream.hasNext()) {
            for (var message : stream.next().getEventsList()) {
                if (message.getMsgType() == CdcMessage.CdcMessageType.DATA) {
                    assertThat(message.getSourceType()).isEqualTo(SourceType.MARIADB);
                    return message;
                }
            }
        }
        throw new AssertionError("MariaDB TLS stream ended before a data event arrived");
    }

    private static void writeCertificate(
            Path path,
            KeyPair key,
            X500Name subject,
            X500Name issuer,
            PrivateKey issuerKey,
            boolean ca)
            throws Exception {
        var now = Instant.now();
        var builder =
                new JcaX509v3CertificateBuilder(
                        issuer,
                        BigInteger.valueOf(ca ? 1 : 2),
                        java.util.Date.from(now.minusSeconds(86400)),
                        java.util.Date.from(now.plusSeconds(86400)),
                        subject,
                        key.getPublic());
        builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(ca));
        if (!ca) {
            builder.addExtension(
                    Extension.subjectAlternativeName,
                    false,
                    new GeneralNames(
                            new GeneralName[] {
                                new GeneralName(GeneralName.dNSName, "localhost"),
                                new GeneralName(GeneralName.iPAddress, "127.0.0.1")
                            }));
        }
        writePem(
                path, builder.build(new JcaContentSignerBuilder("SHA256withRSA").build(issuerKey)));
    }

    private static void writePem(Path path, Object value) throws Exception {
        try (var writer = new JcaPEMWriter(Files.newBufferedWriter(path))) {
            writer.writeObject(value);
        }
    }
}
