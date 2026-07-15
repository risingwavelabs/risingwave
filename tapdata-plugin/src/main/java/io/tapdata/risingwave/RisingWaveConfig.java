package io.tapdata.risingwave;

import io.tapdata.entity.utils.DataMap;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;

/**
 * Immutable connection configuration shared by JDBC and WebSocket paths.
 *
 * <p>Keeping normalization here ensures connection testing and task execution interpret the same
 * TapData form values. Passwords and webhook secrets are deliberately excluded from connection
 * identity and display strings.</p>
 */
final class RisingWaveConfig {
    private final String host;
    private final int port;
    private final String database;
    private final String schema;
    private final String user;
    private final String password;
    private final String ingestMode;
    private final String ingestEndpoint;
    private final String webhookSecret;
    private final String webhookSecretName;
    private final String timezone;
    private final String sslMode;
    private final String extraParameters;

    private RisingWaveConfig(DataMap config) {
        Objects.requireNonNull(config, "config");
        this.host = trimToNull(config.getString("host"));
        this.port = integerValue(config.getObject("port"), 4566);
        this.database = defaultIfBlank(config.getString("database"), "dev");
        this.schema = defaultIfBlank(config.getString("schema"), "public");
        this.user = defaultIfBlank(config.getString("user"), "root");
        this.password = defaultIfNull(config.getString("password"), "");
        this.ingestMode = defaultIfBlank(config.getString("ingest_mode"),
                RisingWaveConnector.MODE_STREAMING);
        this.ingestEndpoint = config.getString("ingestEndpoint");
        this.webhookSecret = config.getString("webhookSecret");
        this.webhookSecretName = trimToNull(config.getString("webhookSecretName"));
        this.timezone = trimToNull(config.getString("timezone"));
        this.sslMode = defaultIfBlank(config.getString("sslmode"), "prefer");
        this.extraParameters = trimToNull(config.getString("extParams"));
    }

    static RisingWaveConfig from(DataMap config) {
        return new RisingWaveConfig(config);
    }

    String host() {
        return host;
    }

    int port() {
        return port;
    }

    String database() {
        return database;
    }

    String schema() {
        return schema;
    }

    String user() {
        return user;
    }

    String password() {
        return password;
    }

    String ingestMode() {
        return ingestMode;
    }

    String resolvedIngestEndpoint() {
        return RisingWaveConnector.resolveIngestEndpoint(ingestEndpoint, host);
    }

    String webhookSecret() {
        return webhookSecret;
    }

    String webhookSecretName() {
        return webhookSecretName;
    }

    String connectionString() {
        return String.format(Locale.ROOT, "%s:%d/%s/%s", host, port, database, schema);
    }

    String instanceUniqueId() {
        // Match the identity fields used by TapData's PostgreSQL connector. Schema is represented
        // separately as a namespace, and write mode does not change the RisingWave instance.
        return sha256(String.join("|", user, String.valueOf(host).toLowerCase(Locale.ROOT),
                Integer.toString(port), database));
    }

    String jdbcUrl() {
        String url = "jdbc:postgresql://" + host + ":" + port + "/" + database
                + "?socketTimeout=30&loginTimeout=30&tcpKeepAlive=true";
        if (extraParameters != null) {
            url += "&" + extraParameters;
        }
        return url;
    }

    Properties jdbcProperties() {
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        properties.setProperty("sslmode", sslMode);
        if (timezone != null) {
            // A driver property preserves offsets such as +08:00 without URL decoding them.
            properties.setProperty("options", "-c timezone=" + timezone);
        }
        return properties;
    }

    private static int integerValue(Object value, int defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        try {
            int parsed = Integer.parseInt(value.toString().trim());
            if (parsed < 1 || parsed > 65535) {
                throw new IllegalArgumentException("Port must be between 1 and 65535");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Port must be a number between 1 and 65535", e);
        }
    }

    private static String defaultIfBlank(String value, String defaultValue) {
        String trimmed = trimToNull(value);
        return trimmed == null ? defaultValue : trimmed;
    }

    private static String defaultIfNull(String value, String defaultValue) {
        return value == null ? defaultValue : value;
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static String sha256(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(digest.length * 2);
            for (byte element : digest) {
                hex.append(String.format(Locale.ROOT, "%02x", element & 0xff));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }
}
