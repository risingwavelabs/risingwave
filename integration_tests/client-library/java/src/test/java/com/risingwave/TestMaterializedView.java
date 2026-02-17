package com.risingwave;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

public class TestMaterializedView {

    public void clearDatabase() throws SQLException {
        try (Connection connection = TestUtils.establishConnection()) {
            // Drop the materialized view
            String dropViewQuery = "DROP MATERIALIZED VIEW IF EXISTS my_materialized_view_java;";
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate(dropViewQuery);
                System.out.println("Materialized view dropped successfully.");
            }
            String truncateTableQuery = "DROP TABLE IF EXISTS my_table_java;";
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate(truncateTableQuery);
                System.out.println("Table my_table_java dropped successfully.");
            }
            truncateTableQuery = "DROP TABLE IF EXISTS test_struct;";
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate(truncateTableQuery);
                System.out.println("Table test_struct dropped successfully.");
            }
        }
    }

    @Test
    public void testStruct() throws SQLException {
        try (Connection conn = TestUtils.establishConnection()) {
            Statement statement = conn.createStatement();
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS test_struct(" +
                    "i1 int [], v1 struct<v2 int, v3 double>, t1 timestamptz, c1 varchar" +
                    ")");

            String insertDataSQL = "INSERT INTO test_struct (i1, v1, t1, c1) VALUES ('{1}', (2, 3), '2020-01-01 01:02:03', 'abc')";
            statement.execute(insertDataSQL);
            statement.execute("FLUSH;");

            QueryRunner runner = new QueryRunner();
            // Build a stable textual representation from struct fields.
            String query =
                    "SELECT i1, '(' || (v1).v2::varchar || ',' || (v1).v3::varchar || ')' AS v1, "
                            + "t1, c1 FROM test_struct";
            List<Map<String, Object>> resultList = runner.query(conn, query, new MapListHandler());
            Assertions.assertEquals(resultList.size(), 1);
            Assertions.assertEquals(resultList.get(0).get("v1"), "(2,3)");
        } finally {
            clearDatabase();
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
