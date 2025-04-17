package com.risingwave;

import org.junit.jupiter.api.Test;

import java.sql.*;

import org.junit.jupiter.api.Assertions;

public class TestCursor {

    public static void createTable() throws SQLException {
        try (Connection connection = TestUtils.establishExtendedConnection()) {
            String createTableSQL = "CREATE TABLE test_table (" +
                    "id INT PRIMARY KEY, " +
                    "trading_date DATE, " +
                    "volume INT)";
            Statement statement = connection.createStatement();
            statement.execute(createTableSQL);

            String insertSQL = "INSERT INTO test_table (id, trading_date, volume) VALUES (1, '2024-07-10', 23)";
            statement.execute(insertSQL);
            System.out.println("Table test_table created successfully.");
        }
    }

    public static void dropTable() throws SQLException {
        String dropSourceQuery = "DROP TABLE test_table;";
        try (Connection connection = TestUtils.establishExtendedConnection()) {
            Statement statement = connection.createStatement();
            statement.executeUpdate(dropSourceQuery);
            System.out.println("Table test_table dropped successfully.");
        }
    }


    public static void readWithExtendedCursor() throws SQLException {
        try (Connection connection = TestUtils.establishExtendedConnection()) {
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            statement.execute("START TRANSACTION ISOLATION LEVEL REPEATABLE READ");

            String declareCursorSql = "DECLARE c1 CURSOR FOR SELECT id, trading_date, volume FROM public.test_table WHERE ((id = CAST(? AS INT)))";
            PreparedStatement pstmt = connection.prepareStatement(declareCursorSql);
            pstmt.setInt(1, 1);
            pstmt.execute();

            statement.execute("FETCH 100 FROM c1");
            ResultSet resultSet = statement.getResultSet();

            while (resultSet != null && resultSet.next()) {
                Assertions.assertEquals(resultSet.getInt("id"), 1);
                Assertions.assertEquals(resultSet.getString("trading_date"), "2024-07-10");
                Assertions.assertEquals(resultSet.getInt("volume"), 23);
            }

            statement.execute("CLOSE c1");
            statement.execute("COMMIT");

            System.out.println("Data in table read with cursor successfully.");
        }
    }

    public static void readWithExtendedSubscriptionCursor() throws SQLException {
        try (Connection connection = TestUtils.establishExtendedConnection()) {
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            statement.execute("START TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            statement.execute("CREATE SUBSCRIPTION sub FROM public.test_table WITH(retention = '1d')");

            String declareCursorSql = "DECLARE c1 SUBSCRIPTION CURSOR FOR sub FULL";
            PreparedStatement pstmt = connection.prepareStatement(declareCursorSql);
            pstmt.execute();

            statement.execute("FETCH 100 FROM c1");
            ResultSet resultSet = statement.getResultSet();

            while (resultSet != null && resultSet.next()) {
                Assertions.assertEquals(resultSet.getInt("id"), 1);
                Assertions.assertEquals(resultSet.getString("trading_date"), "2024-07-10");
                Assertions.assertEquals(resultSet.getInt("volume"), 23);
            }

            statement.execute("CLOSE c1");
            statement.execute("COMMIT");
            statement.execute("DROP SUBSCRIPTION sub");

            System.out.println("Data in table read with cursor successfully.");
        }
    }

    @Test
    public void testCursor() throws SQLException {
        createTable();
        readWithExtendedCursor();
        dropTable();
    }

    @Test
    public void testSubscriptionCursor() throws SQLException {
        createTable();
        readWithExtendedSubscriptionCursor();
        dropTable();
    }
}
