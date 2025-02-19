package com.risingwave;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class TestUtils {
    public static Connection establishConnection() throws SQLException {
        // TODO: remove preferQueryMode=simple.
        final String url = "jdbc:postgresql://risingwave-standalone:4566/dev?preferQueryMode=simple";
        final String user = "root";
        final String password = "";

        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);

        return DriverManager.getConnection(url, props);
    }

    public static Connection establishExtendedConnection() throws SQLException {
        final String url = "jdbc:postgresql://risingwave-standalone:4566/dev";
        final String user = "root";
        final String password = "";

        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);

        return DriverManager.getConnection(url, props);
    }
}
