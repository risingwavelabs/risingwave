package com.risingwave;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TestMaterializedView {

    public void clearDatabase() throws SQLException {
        try (Connection connection = TestUtils.establishConnection()) {
            // Drop the materialized view
            String dropViewQuery = "DROP MATERIALIZED VIEW IF EXISTS my_materialized_view_java;";
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate(dropViewQuery);
                System.out.println("Materialized view dropped successfully.");
            }
            String truncateTableQuery = "DROP TABLE my_table_java;";
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate(truncateTableQuery);
                System.out.println("Table dropped successfully.");
            }
        }
    }

    @Test
    public void testCompatibility() throws SQLException {
        try (Connection connection = TestUtils.establishConnection()) {
            Statement statement = connection.createStatement();

            String createTableSQL = "CREATE TABLE IF NOT EXISTS my_table_java (id int, name VARCHAR)";
            statement.executeUpdate(createTableSQL);

            String insertDataSQL = "INSERT INTO my_table_java (id,name) VALUES (1,'John'), (2,'Jane'), (3,'Alice')";
            statement.execute(insertDataSQL);
            statement.execute("FLUSH;");

            String viewName = "my_materialized_view_java";
            String createViewSQL = "CREATE MATERIALIZED VIEW " + viewName + " AS SELECT * FROM my_table_java";
            statement.execute(createViewSQL);

            String query = "SELECT * FROM " + viewName;
            ResultSet resultSet = statement.executeQuery(query);

            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String name = resultSet.getString("name");
                System.out.println("ID: " + id + ", Name: " + name);
            }

            String updateDataSQL = "UPDATE my_table_java SET name = 'Bob' WHERE id = 1";
            statement.execute(updateDataSQL);

            resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String name = resultSet.getString("name");
                System.out.println("ID: " + id + ", Name: " + name);
            }
        }

        clearDatabase();
    }
}
