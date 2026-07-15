package io.tapdata.risingwave;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

/** Manages the catalog Secret referenced by webhook validation expressions. */
final class RisingWaveWebhookSecret {
    private static final String AUTO_PREFIX = "tapdata_webhook_";

    private RisingWaveWebhookSecret() {
    }

    static Handle prepare(Connection connection, String schema, String tableId,
                          String configuredName, String value) throws SQLException {
        if (value == null || value.isEmpty()) {
            return Handle.disabled();
        }
        if (configuredName != null && !configuredName.trim().isEmpty()) {
            return new Handle(validateName(configuredName.trim()), false);
        }

        String name = automaticName(schema, tableId);
        String qualifiedName = RisingWaveSql.quoteIdentifier(schema) + "."
                + RisingWaveSql.quoteIdentifier(name);
        String literal = RisingWaveSql.quoteStringLiteral(value);
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE SECRET IF NOT EXISTS " + qualifiedName
                    + " WITH (backend = 'meta') AS " + literal);
            // CREATE IF NOT EXISTS preserves an old payload. ALTER makes retries and secret
            // rotation deterministic while keeping the value out of SHOW CREATE TABLE output.
            statement.execute("ALTER SECRET " + qualifiedName + " AS " + literal);
        } catch (SQLException error) {
            throw new SQLException("Unable to create the protected RisingWave Secret used for "
                    + "webhook validation. Ensure Secret Management is available and the user "
                    + "has CREATE SECRET permission, or configure an existing Webhook Secret Name: "
                    + error.getMessage(), error.getSQLState(), error.getErrorCode(), error);
        }
        return new Handle(name, true);
    }

    static void dropManaged(Connection connection, String schema, Handle handle) throws SQLException {
        if (handle == null || !handle.managed()) {
            return;
        }
        try (Statement statement = connection.createStatement()) {
            statement.execute("DROP SECRET IF EXISTS " + RisingWaveSql.quoteIdentifier(schema)
                    + "." + RisingWaveSql.quoteIdentifier(handle.name()));
        }
    }

    static void dropAutomatic(Connection connection, String schema, String tableId) throws SQLException {
        dropManaged(connection, schema, new Handle(automaticName(schema, tableId), true));
    }

    static String automaticName(String schema, String tableId) {
        return AUTO_PREFIX + sha256(schema + "\u0000" + tableId).substring(0, 32);
    }

    private static String validateName(String name) {
        if (!name.matches("[A-Za-z_][A-Za-z0-9_$]*")) {
            throw new IllegalArgumentException("Webhook Secret Name must be an unqualified "
                    + "RisingWave identifier in the configured schema");
        }
        return name;
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
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 is unavailable", error);
        }
    }

    static final class Handle {
        private final String name;
        private final boolean managed;

        private Handle(String name, boolean managed) {
            this.name = name;
            this.managed = managed;
        }

        static Handle disabled() {
            return new Handle(null, false);
        }

        String name() {
            return name;
        }

        boolean managed() {
            return managed;
        }

        boolean enabled() {
            return name != null;
        }
    }
}
