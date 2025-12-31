/*
 * Copyright 2025 RisingWave Labs
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

package com.risingwave.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.io.StringReader;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.Security;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Snowflake-specific JDBC sink configuration with support for key-pair authentication. Extends
 * JDBCSinkConfig with Snowflake-specific authentication fields.
 */
public class SnowflakeJDBCSinkConfig extends JDBCSinkConfig {
    private static final Logger LOG = LoggerFactory.getLogger(SnowflakeJDBCSinkConfig.class);

    private static final String PROP_PRIVATE_KEY = "privateKey";
    private static final String PROP_PRIVATE_KEY_PEM = "private_key_pem";
    private static final String PROP_PRIVATE_KEY_PWD = "private_key_file_pwd";
    private static final String PROP_AUTH_METHOD = "auth.method";
    private static final String AUTH_METHOD_KEY_PAIR_OBJECT = "key_pair_object";

    static {
        if (Security.getProvider("BC") == null) {
            Security.addProvider(new BouncyCastleProvider());
        }
    }

    // Authentication method control (password | key_pair_file | key_pair_object)
    @JsonProperty(value = "auth.method")
    private String authMethod;

    // Key-pair authentication via connection Properties (file-based)
    @JsonProperty(value = "private_key_file")
    private String privateKeyFile;

    @JsonProperty(value = "private_key_file_pwd")
    private String privateKeyFilePwd;

    // Key-pair authentication via connection Properties (object-based, PEM content)
    @JsonProperty(value = "private_key_pem")
    private String privateKeyPem;

    @JsonCreator
    public SnowflakeJDBCSinkConfig(
            @JsonProperty(value = "jdbc.url") String jdbcUrl,
            @JsonProperty(value = "table.name") String tableName,
            @JsonProperty(value = "type") String sinkType) {
        super(jdbcUrl, tableName, sinkType);
    }

    public String getAuthMethod() {
        return authMethod;
    }

    public String getPrivateKeyFile() {
        return privateKeyFile;
    }

    public String getPrivateKeyFilePwd() {
        return privateKeyFilePwd;
    }

    public String getPrivateKeyPem() {
        return privateKeyPem;
    }

    /**
     * Creates a Snowflake JDBC connection with support for key-pair authentication. Overrides the
     * base implementation to handle Snowflake-specific auth methods.
     *
     * @return JDBC connection configured for Snowflake
     * @throws SQLException if connection fails
     */
    @Override
    public Connection getConnection() throws SQLException {
        String jdbcUrl = getJdbcUrl();
        // Use shared helper to create base properties
        var props = JdbcUtils.createBaseProperties(jdbcUrl, getUser());

        // Set authentication-related properties
        if (authMethod != null) {
            props.put(PROP_AUTH_METHOD, authMethod);
        }
        if (getPassword() != null) {
            props.put("password", getPassword());
        }
        if (privateKeyFile != null) {
            props.put("private_key_file", privateKeyFile);
        }
        if (privateKeyPem != null) {
            props.put(PROP_PRIVATE_KEY_PEM, privateKeyPem);
        }
        if (privateKeyFilePwd != null && !privateKeyFilePwd.isEmpty()) {
            props.put(PROP_PRIVATE_KEY_PWD, privateKeyFilePwd);
        }

        // Handle Snowflake-specific authentication
        try {
            handleSnowflakeAuth(props);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("Failed to configure Snowflake authentication", e);
            throw new SQLException("Failed to configure authentication: " + e.getMessage(), e);
        }

        var conn = DriverManager.getConnection(jdbcUrl, props);
        conn.setAutoCommit(isAutoCommit());
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        return conn;
    }

    /**
     * Handles Snowflake-specific authentication by processing the auth.method property and setting
     * up appropriate authentication properties. This is the main entry point for Snowflake
     * authentication configuration.
     *
     * <p>Supported authentication methods: - password: Standard username/password authentication -
     * key_pair_file: Key-pair authentication using a private key file path - key_pair_object:
     * Key-pair authentication using PEM content (converted to PrivateKey object)
     *
     * @param props Properties to configure (will be modified in-place). Expected properties:
     *     auth.method, password, private_key_file, private_key_pem, private_key_file_pwd
     * @throws IOException if PEM parsing fails
     * @throws GeneralSecurityException if key conversion fails
     * @throws OperatorCreationException if decryption fails
     * @throws PKCSException if PKCS decryption fails
     * @throws SQLException if authentication setup fails
     */
    public static void handleSnowflakeAuth(Properties props)
            throws IOException,
                    GeneralSecurityException,
                    OperatorCreationException,
                    PKCSException,
                    SQLException {
        String authMethod = props.getProperty(PROP_AUTH_METHOD);
        if (authMethod == null) {
            // No auth method specified, use default password authentication
            return;
        }

        props.remove(PROP_AUTH_METHOD);

        if ("password".equalsIgnoreCase(authMethod)) {
            // Password authentication - no additional processing needed
            // The password property is already set
        } else if ("key_pair_file".equalsIgnoreCase(authMethod)) {
            // File-based key-pair authentication
            // The private_key_file and optional private_key_file_pwd properties are already set
            props.put("authenticator", "snowflake_jwt");
        } else if (AUTH_METHOD_KEY_PAIR_OBJECT.equalsIgnoreCase(authMethod)) {
            // Object-based key-pair authentication - convert PEM to PrivateKey object
            String pem = props.getProperty(PROP_PRIVATE_KEY_PEM);
            String passphrase = props.getProperty(PROP_PRIVATE_KEY_PWD);

            PrivateKey privateKey = loadPrivateKeyFromPem(pem, passphrase);
            props.put(PROP_PRIVATE_KEY, privateKey);
            props.remove(PROP_PRIVATE_KEY_PEM);
            props.remove(PROP_PRIVATE_KEY_PWD);
            props.put("authenticator", "snowflake_jwt");
            LOG.debug("Loaded private key for Snowflake authentication");
        }
    }

    public static PrivateKey loadPrivateKeyFromPem(String pemContent, String passphrase)
            throws IOException, GeneralSecurityException, OperatorCreationException, PKCSException {
        try (PEMParser parser = new PEMParser(new StringReader(pemContent))) {
            Object parsed = parser.readObject();
            if (parsed == null) {
                throw new GeneralSecurityException("No key found in privateKeyPem content");
            }
            return convertToPrivateKey(parsed, passphrase);
        }
    }

    public static PrivateKey convertToPrivateKey(Object parsed, String passphrase)
            throws GeneralSecurityException, IOException, OperatorCreationException, PKCSException {
        JcaPEMKeyConverter converter = new JcaPEMKeyConverter();
        converter.setProvider("BC");

        if (parsed instanceof PrivateKeyInfo) {
            PrivateKeyInfo info = (PrivateKeyInfo) parsed;
            return converter.getPrivateKey(info);
        }
        if (parsed instanceof PKCS8EncryptedPrivateKeyInfo) {
            PKCS8EncryptedPrivateKeyInfo encryptedInfo = (PKCS8EncryptedPrivateKeyInfo) parsed;
            if (passphrase == null || passphrase.isEmpty()) {
                throw new GeneralSecurityException(
                        "Encrypted private key provided but 'private_key_file_pwd' is missing");
            }
            InputDecryptorProvider decryptorProvider =
                    new JceOpenSSLPKCS8DecryptorProviderBuilder().build(passphrase.toCharArray());
            return converter.getPrivateKey(encryptedInfo.decryptPrivateKeyInfo(decryptorProvider));
        }

        // Some PEMs may be base64 without headers; attempt to decode as DER PKCS#8
        if (parsed instanceof byte[]) {
            try {
                PrivateKeyInfo info = PrivateKeyInfo.getInstance((byte[]) parsed);
                return converter.getPrivateKey(info);
            } catch (Exception e) {
                throw new GeneralSecurityException("Unsupported private key format", e);
            }
        }

        throw new GeneralSecurityException(
                "Unsupported private key object type: " + parsed.getClass().getName());
    }
}
