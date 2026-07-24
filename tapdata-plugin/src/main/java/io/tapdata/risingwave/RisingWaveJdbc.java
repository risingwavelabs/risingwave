package io.tapdata.risingwave;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Opens RisingWave PostgreSQL-wire connections inside TapData's PDK classloader. */
final class RisingWaveJdbc {
    private RisingWaveJdbc() {
    }

    static Connection open(RisingWaveConfig config) throws SQLException {
        try {
            // ServiceLoader discovery is not reliable for drivers inside a shaded PDK JAR.
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new SQLException("PostgreSQL JDBC driver not found in classpath", e);
        }
        return DriverManager.getConnection(config.jdbcUrl(), config.jdbcProperties());
    }
}
