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

import com.google.common.collect.Iterators;
import com.risingwave.connector.JDBCSink;
import com.risingwave.connector.JDBCSinkConfig;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.ArraySinkRow;
import com.risingwave.proto.Data.Op;
import java.sql.*;
import org.junit.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;

public class JDBCSinkTest {
    static void createMockTable(String jdbcUrl, String tableName) throws SQLException {
        Connection conn = DriverManager.getConnection(jdbcUrl);
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();
        stmt.execute("DROP TABLE IF EXISTS " + tableName);
        stmt.execute("create table " + tableName + " (id int primary key, name varchar(255))");
        conn.commit();
        conn.close();
    }

    static void testJDBCSync(JdbcDatabaseContainer<?> container) throws SQLException {
        String tableName = "test";
        createMockTable(container.getJdbcUrl(), tableName);

        JDBCSink sink =
                new JDBCSink(
                        new JDBCSinkConfig(container.getJdbcUrl(), tableName, "upsert"),
                        TableSchema.getMockTableSchema());
        assertEquals(tableName, sink.getTableName());
        Connection conn = sink.getConn();

        sink.write(Iterators.forArray(new ArraySinkRow(Op.INSERT, 1, "Alice")));
        sink.sync();

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM test");
        int count;
        for (count = 0; rs.next(); ) {
            count++;
        }
        assertEquals(1, count);

        sink.write(Iterators.forArray(new ArraySinkRow(Op.INSERT, 2, "Bob")));
        sink.sync();
        stmt = conn.createStatement();
        rs = stmt.executeQuery("SELECT * FROM test");
        for (count = 0; rs.next(); ) {
            count++;
        }
        assertEquals(2, count);

        sink.sync();
        sink.drop();
    }

    static void testJDBCWrite(JdbcDatabaseContainer<?> container) throws SQLException {
        String tableName = "test";
        createMockTable(container.getJdbcUrl(), tableName);

        JDBCSink sink =
                new JDBCSink(
                        new JDBCSinkConfig(container.getJdbcUrl(), tableName, "upsert"),
                        TableSchema.getMockTableSchema());
        assertEquals(tableName, sink.getTableName());
        Connection conn = sink.getConn();

        sink.write(
                Iterators.forArray(
                        new ArraySinkRow(Op.INSERT, 1, "Alice"),
                        new ArraySinkRow(Op.INSERT, 2, "Bob"),
                        new ArraySinkRow(Op.UPDATE_DELETE, 1, "Alice"),
                        new ArraySinkRow(Op.UPDATE_INSERT, 1, "Clare"),
                        new ArraySinkRow(Op.DELETE, 2, "Bob")));
        sink.sync();

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM test");
        rs.next();

        // check if rows are inserted
        assertEquals(1, rs.getInt(1));
        assertEquals("Clare", rs.getString(2));
        assertFalse(rs.next());

        sink.sync();
        stmt.close();
    }

    static void testJDBCDrop(JdbcDatabaseContainer<?> container) throws SQLException {
        String tableName = "test";
        createMockTable(container.getJdbcUrl(), tableName);

        JDBCSink sink =
                new JDBCSink(
                        new JDBCSinkConfig(container.getJdbcUrl(), tableName, "upsert"),
                        TableSchema.getMockTableSchema());
        assertEquals(tableName, sink.getTableName());
        Connection conn = sink.getConn();
        sink.drop();
        try {
            assertTrue(conn.isClosed());
        } catch (SQLException e) {
            fail(String.valueOf(e));
        }
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
        testJDBCSync(pg);
        testJDBCWrite(pg);
        testJDBCDrop(pg);
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
        testJDBCSync(mysql);
        testJDBCWrite(mysql);
        testJDBCDrop(mysql);
        mysql.stop();
    }
}
