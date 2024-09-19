package com.risingwave;

import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class TestDatabaseConnection {
    static Connection establishConnection() throws SQLException {
        final String url = "jdbc:postgresql://risingwave-standalone:4566/dev";

        Properties props = new Properties();
        props.setProperty("user", "root");
        props.setProperty("password", "");
        props.setProperty("sslmode", "disable");
        // To reproduce the bug: https://github.com/risingwavelabs/metabase-risingwave-driver/issues/1
        props.setProperty("ApplicationName", "01234567890123456789012345678901234567890123456");

        return DriverManager.getConnection(url, props);
    }

    @Test
    public void testEstablishConnection() throws SQLException {
        try (Connection conn = TestUtils.establishConnection()) {
            assertNotNull(conn, "Connection should not be null");

            String query = "SELECT 1";
            Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(query);
            assertTrue(resultSet.next(), "Expected a result");
            int resultValue = resultSet.getInt(1);
            assertEquals(1, resultValue, "Expected result value to be 1");
        }
    }
}
