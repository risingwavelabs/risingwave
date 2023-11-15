// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.risingwave.connector.sink.jdbc;

import static org.junit.Assert.*;

import com.google.common.collect.Lists;
import com.risingwave.connector.JDBCSink;
import com.risingwave.connector.JDBCSinkConfig;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.ArraySinkRow;
import com.risingwave.proto.Data;
import com.risingwave.proto.Data.DataType.TypeName;
import com.risingwave.proto.Data.Op;
import java.sql.*;
import java.util.List;
import org.junit.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;

public class JDBCSinkTest {
    private enum TestType {
        TestPg,
        TestMySQL,
    }

    private static final String pgCreateStmt =
            "CREATE TABLE %s (id INT PRIMARY KEY, v_varchar VARCHAR(255), v_date DATE, v_time TIME, v_timestamp TIMESTAMP, v_jsonb JSONB, v_bytea BYTEA)";
    private static final String mysqlCreateStmt =
            "CREATE TABLE %s (id INT PRIMARY KEY, v_varchar VARCHAR(255), v_date DATE, v_time TIME, v_timestamp TIMESTAMP, v_jsonb JSON, v_bytea BLOB)";

    static void createMockTable(String jdbcUrl, String tableName, TestType testType)
            throws SQLException {
        Connection conn = DriverManager.getConnection(jdbcUrl);
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();
        stmt.execute("DROP TABLE IF EXISTS " + tableName);
        if (testType == TestType.TestPg) {
            stmt.execute(String.format(pgCreateStmt, tableName));
        } else {
            stmt.execute(String.format(mysqlCreateStmt, tableName));
        }
        conn.commit();
        conn.close();
    }

    static TableSchema getTestTableSchema() {
        return new TableSchema(
                Lists.newArrayList(
                        "id", "v_varchar", "v_date", "v_time", "v_timestamp", "v_jsonb", "v_bytea"),
                Lists.newArrayList(
                        Data.DataType.newBuilder().setTypeName(TypeName.INT32).build(),
                        Data.DataType.newBuilder().setTypeName(TypeName.VARCHAR).build(),
                        Data.DataType.newBuilder().setTypeName(TypeName.DATE).build(),
                        Data.DataType.newBuilder().setTypeName(TypeName.TIME).build(),
                        Data.DataType.newBuilder().setTypeName(TypeName.TIMESTAMP).build(),
                        Data.DataType.newBuilder().setTypeName(TypeName.JSONB).build(),
                        Data.DataType.newBuilder().setTypeName(TypeName.BYTEA).build()),
                Lists.newArrayList("id"));
    }

    static void testJDBCSync(JdbcDatabaseContainer<?> container, TestType testType)
            throws SQLException {
        String tableName = "test2";
        createMockTable(container.getJdbcUrl(), tableName, testType);
        JDBCSink sink =
                new JDBCSink(
                        new JDBCSinkConfig(container.getJdbcUrl(), tableName, "upsert"),
                        getTestTableSchema());
        assertEquals(tableName, sink.getTableName());
        Connection conn = DriverManager.getConnection(container.getJdbcUrl());

        sink.write(
                List.of(
                        new ArraySinkRow(
                                Op.INSERT,
                                1,
                                "Alice",
                                new Date(1000000000),
                                new Time(1000000000),
                                new Timestamp(1000000000),
                                "{\"key\": \"password\", \"value\": \"Singularity123\"}",
                                "I want to sleep".getBytes())));
        sink.barrier(true);

        Statement stmt = conn.createStatement();
        try (var rs = stmt.executeQuery(String.format("SELECT * FROM %s", tableName))) {
            int count;
            for (count = 0; rs.next(); ) {
                count++;
            }
            assertEquals(1, count);
        }

        sink.write(
                List.of(
                        new ArraySinkRow(
                                Op.INSERT,
                                2,
                                "Bob",
                                new Date(1000000000),
                                new Time(1000000000),
                                new Timestamp(1000000000),
                                "{\"key\": \"password\", \"value\": \"Singularity123\"}",
                                "I want to sleep".getBytes())));
        sink.barrier(true);
        try (var rs = stmt.executeQuery(String.format("SELECT * FROM %s", tableName))) {
            int count;
            for (count = 0; rs.next(); ) {
                count++;
            }
            assertEquals(2, count);
        }
        stmt.close();
        conn.close();

        sink.barrier(true);
        sink.drop();
    }

    static void testJDBCWrite(JdbcDatabaseContainer<?> container, TestType testType)
            throws SQLException {
        String tableName = "test1";
        createMockTable(container.getJdbcUrl(), tableName, testType);

        JDBCSink sink =
                new JDBCSink(
                        new JDBCSinkConfig(container.getJdbcUrl(), tableName, "upsert"),
                        getTestTableSchema());
        assertEquals(tableName, sink.getTableName());
        Connection conn = DriverManager.getConnection(container.getJdbcUrl());
        Statement stmt = conn.createStatement();

        sink.write(
                List.of(
                        new ArraySinkRow(
                                Op.INSERT,
                                1,
                                "Alice",
                                new Date(1000000000),
                                new Time(1000000000),
                                new Timestamp(1000000000),
                                "{\"key\": \"password\", \"value\": \"Singularity123\"}",
                                "I want to sleep".getBytes()),
                        new ArraySinkRow(
                                Op.INSERT,
                                2,
                                "Bob",
                                new Date(1000000000),
                                new Time(1000000000),
                                new Timestamp(1000000000),
                                "{\"key\": \"password\", \"value\": \"Singularity123\"}",
                                "I want to sleep".getBytes())));

        // chunk will commit after sink.write()
        try (var rs = stmt.executeQuery(String.format("SELECT COUNT(*) FROM %s", tableName))) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }

        sink.write(
                List.of(
                        new ArraySinkRow(
                                Op.UPDATE_DELETE,
                                1,
                                "Alice",
                                new Date(1000000000),
                                new Time(1000000000),
                                new Timestamp(1000000000),
                                "{\"key\": \"password\", \"value\": \"Singularity123\"}",
                                "I want to sleep".getBytes()),
                        new ArraySinkRow(
                                Op.UPDATE_INSERT,
                                1,
                                "Clare",
                                new Date(2000000000),
                                new Time(2000000000),
                                new Timestamp(2000000000),
                                "{\"key\": \"password\", \"value\": \"Singularity123123123123\"}",
                                "I want to eat".getBytes()),
                        new ArraySinkRow(
                                Op.DELETE,
                                2,
                                "Bob",
                                new Date(1000000000),
                                new Time(1000000000),
                                new Timestamp(1000000000),
                                "{\"key\": \"password\", \"value\": \"Singularity123\"}",
                                "I want to sleep".getBytes())));

        try (var rs = stmt.executeQuery(String.format("SELECT * FROM %s", tableName))) {
            assertTrue(rs.next());

            // check if rows are inserted
            assertEquals(1, rs.getInt(1));
            assertEquals("Clare", rs.getString(2));
            assertEquals(new Date(2000000000).toString(), rs.getDate(3).toString());
            assertEquals(new Time(2000000000).toString(), rs.getTime(4).toString());
            assertEquals(new Timestamp(2000000000), rs.getTimestamp(5));
            assertEquals(
                    "{\"key\": \"password\", \"value\": \"Singularity123123123123\"}",
                    rs.getString(6));
            assertEquals("I want to eat", new String(rs.getBytes(7)));
            assertFalse(rs.next());
        }

        sink.barrier(true);
        stmt.close();
        conn.close();
    }

    static void testJDBCDrop(JdbcDatabaseContainer<?> container, TestType testType)
            throws SQLException {
        String tableName = "test3";
        createMockTable(container.getJdbcUrl(), tableName, testType);

        JDBCSink sink =
                new JDBCSink(
                        new JDBCSinkConfig(container.getJdbcUrl(), tableName, "upsert"),
                        getTestTableSchema());
        assertEquals(tableName, sink.getTableName());
        Connection conn = sink.getConn();
        sink.drop();
        assertTrue(conn.isClosed());
    }

    @Test
    public void testPostgres() throws SQLException {
        PostgreSQLContainer pg =
                new PostgreSQLContainer<>("postgres:15-alpine")
                        .withDatabaseName("test")
                        .withUsername("postgres")
                        .withPassword("password")
                        .withDatabaseName("test_db")
                        .withUrlParam("user", "postgres")
                        .withUrlParam("password", "password");
        pg.start();
        testJDBCWrite(pg, TestType.TestPg);
        testJDBCSync(pg, TestType.TestPg);
        testJDBCDrop(pg, TestType.TestPg);
        pg.stop();
    }

    @Test
    public void testMySQL() throws SQLException {
        MySQLContainer mysql =
                new MySQLContainer<>("mysql:8")
                        .withDatabaseName("test")
                        .withUsername("postgres")
                        .withPassword("password")
                        .withDatabaseName("test_db")
                        .withUrlParam("user", "postgres")
                        .withUrlParam("password", "password");
        mysql.start();
        testJDBCWrite(mysql, TestType.TestMySQL);
        testJDBCSync(mysql, TestType.TestMySQL);
        testJDBCDrop(mysql, TestType.TestMySQL);
        mysql.stop();
    }
}
