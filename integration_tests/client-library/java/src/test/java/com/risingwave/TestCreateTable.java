package com.risingwave;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class TestCreateTable {

    public static void createSourceTable() throws SQLException {
        String createTableQuery;
        Statement statement;
        try (Connection connection = TestUtils.establishConnection()) {
            createTableQuery = "CREATE TABLE s1_java (i1 int [], v1 struct<v2 int, v3 double>, t1 timestamp, c1 varchar) " +
                    "WITH (" +
                    "    connector = 'datagen'," +
                    "    fields.i1.length = '3'," +
                    "    fields.i1._.kind = 'sequence'," +
                    "    fields.i1._.start = '1'," +
                    "    fields.v1.v2.kind = 'random'," +
                    "    fields.v1.v2.min = '-10'," +
                    "    fields.v1.v2.max = '10'," +
                    "    fields.v1.v2.seed = '1'," +
                    "    fields.v1.v3.kind = 'random'," +
                    "    fields.v1.v3.min = '15'," +
                    "    fields.v1.v3.max = '55'," +
                    "    fields.v1.v3.seed = '1'," +
                    "    fields.t1.kind = 'random'," +
                    "    fields.t1.max_past = '10 day'," +
                    "    fields.t1.seed = '3'," +
                    "    fields.c1.kind = 'random'," +
                    "    fields.c1.length = '16'," +
                    "    fields.c1.seed = '3'," +
                    "    datagen.rows.per.second = '10'" +
                    ") FORMAT plain ENCODE json ;";
            statement = connection.createStatement();
            statement.executeUpdate(createTableQuery);
            System.out.println("Source table s1_java created successfully.");
        }
    }

    public static void dropSourceTable() throws SQLException {
        String dropSourceQuery = "DROP TABLE s1_java;";
        try (Connection connection = TestUtils.establishConnection()) {
            Statement statement = connection.createStatement();
            statement.executeUpdate(dropSourceQuery);
            System.out.println("Source table s1_java dropped successfully.");
        }
    }

    @Test
    public void testCreateSourceTable() throws SQLException {
        createSourceTable();
        dropSourceTable();
    }
}
