package io.tapdata.risingwave;

import io.tapdata.entity.utils.DataMap;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RisingWaveConfigTest {

    @Test
    void appliesDefaultsAndBuildsPasswordFreePdkIdentity() {
        DataMap values = new DataMap();
        values.put("host", " RisingWave.EXAMPLE.com ");
        values.put("password", "do-not-expose");
        RisingWaveConfig config = RisingWaveConfig.from(values);

        assertEquals("RisingWave.EXAMPLE.com:4566/dev/public", config.connectionString());
        assertEquals(64, config.instanceUniqueId().length());
        assertFalse(config.connectionString().contains("do-not-expose"));
        assertFalse(config.instanceUniqueId().contains("do-not-expose"));
        assertEquals("ws://RisingWave.EXAMPLE.com:4560", config.resolvedIngestEndpoint());
    }

    @Test
    void buildsJdbcSettingsFromNormalizedFormValues() {
        DataMap values = new DataMap();
        values.put("host", "rw.example.com");
        values.put("port", 15432);
        values.put("database", "analytics");
        values.put("schema", "sink");
        values.put("user", "tapdata");
        values.put("password", "secret");
        values.put("sslmode", "require");
        values.put("timezone", "+08:00");
        values.put("extParams", "applicationName=tapdata");
        RisingWaveConfig config = RisingWaveConfig.from(values);

        assertEquals("jdbc:postgresql://rw.example.com:15432/analytics"
                + "?socketTimeout=30&loginTimeout=30&tcpKeepAlive=true&applicationName=tapdata",
                config.jdbcUrl());
        Properties properties = config.jdbcProperties();
        assertEquals("tapdata", properties.getProperty("user"));
        assertEquals("secret", properties.getProperty("password"));
        assertEquals("require", properties.getProperty("sslmode"));
        assertEquals("-c timezone=+08:00", properties.getProperty("options"));
    }

    @Test
    void instanceIdentityIsStableAcrossModesAndChangesAcrossInstances() {
        DataMap firstValues = baseValues();
        firstValues.put("ingest_mode", RisingWaveConnector.MODE_STREAMING);
        RisingWaveConfig first = RisingWaveConfig.from(firstValues);

        DataMap sameInstanceValues = baseValues();
        sameInstanceValues.put("ingest_mode", RisingWaveConnector.MODE_JDBC);
        sameInstanceValues.put("schema", "another_schema");
        sameInstanceValues.put("password", "another-password");
        sameInstanceValues.put("webhookSecret", "another-webhook-secret");
        sameInstanceValues.put("webhookSecretName", "existing_secret");
        RisingWaveConfig sameInstance = RisingWaveConfig.from(sameInstanceValues);

        DataMap otherDatabaseValues = baseValues();
        otherDatabaseValues.put("database", "other");
        RisingWaveConfig otherDatabase = RisingWaveConfig.from(otherDatabaseValues);

        assertEquals(first.instanceUniqueId(), sameInstance.instanceUniqueId());
        assertEquals("existing_secret", sameInstance.webhookSecretName());
        assertNotEquals(first.instanceUniqueId(), otherDatabase.instanceUniqueId());

        DataMap otherHostValues = baseValues();
        otherHostValues.put("host", "other.example.com");
        assertNotEquals(first.instanceUniqueId(),
                RisingWaveConfig.from(otherHostValues).instanceUniqueId());

        DataMap otherPortValues = baseValues();
        otherPortValues.put("port", 4567);
        assertNotEquals(first.instanceUniqueId(),
                RisingWaveConfig.from(otherPortValues).instanceUniqueId());

        DataMap otherUserValues = baseValues();
        otherUserValues.put("user", "another-user");
        assertNotEquals(first.instanceUniqueId(),
                RisingWaveConfig.from(otherUserValues).instanceUniqueId());
    }

    @Test
    void rejectsNonNumericAndOutOfRangePorts() {
        DataMap nonNumeric = baseValues();
        nonNumeric.put("port", "invalid");
        IllegalArgumentException nonNumericError = assertThrows(IllegalArgumentException.class,
                () -> RisingWaveConfig.from(nonNumeric));
        assertEquals("Port must be a number between 1 and 65535", nonNumericError.getMessage());

        DataMap outOfRange = baseValues();
        outOfRange.put("port", 65536);
        IllegalArgumentException rangeError = assertThrows(IllegalArgumentException.class,
                () -> RisingWaveConfig.from(outOfRange));
        assertEquals("Port must be between 1 and 65535", rangeError.getMessage());
    }

    @Test
    void acceptsIntegralJsonNumbersForPortAndRejectsFractions() {
        DataMap integral = baseValues();
        integral.put("port", 4566.0d);
        assertEquals(4566, RisingWaveConfig.from(integral).port());

        DataMap fractional = baseValues();
        fractional.put("port", 4566.5d);
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> RisingWaveConfig.from(fractional));
        assertEquals("Port must be a whole number between 1 and 65535", error.getMessage());
    }

    @Test
    void rejectsUnknownWriteModesInsteadOfFallingBackToJdbc() {
        DataMap values = baseValues();
        values.put("ingest_mode", "streamng");

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> RisingWaveConfig.from(values));

        assertTrue(error.getMessage().contains("Unsupported Write Mode"));
        assertTrue(error.getMessage().contains("streamng"));
    }

    private static DataMap baseValues() {
        DataMap values = new DataMap();
        values.put("host", "rw.example.com");
        values.put("port", 4566);
        values.put("database", "dev");
        values.put("schema", "public");
        values.put("user", "root");
        return values;
    }
}
