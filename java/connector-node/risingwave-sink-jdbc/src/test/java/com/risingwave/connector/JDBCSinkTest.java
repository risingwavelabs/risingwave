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

package com.risingwave.connector;

import static org.junit.Assert.*;

import com.google.common.collect.Iterators;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.ArraySinkrow;
import com.risingwave.proto.Data.Op;
import java.sql.*;
import org.junit.Test;

public class JDBCSinkTest {
    static String dbName = "test_db";
    static String connectionURL = "jdbc:derby:" + dbName + ";create=true";

    @Test
    public void testJDBCSync() throws SQLException {
        Connection conn = DriverManager.getConnection(connectionURL);
        JDBCSink sink = new JDBCSink(conn, TableSchema.getMockTableSchema(), "test");
        createMockTable(conn, sink.getTableName());

        sink.write(Iterators.forArray(new ArraySinkrow(Op.INSERT, 1, "Alice")));
        sink.sync();

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM test");
        int count;
        for (count = 0; rs.next(); ) {
            count++;
        }
        assertEquals(1, count);

        sink.write(Iterators.forArray(new ArraySinkrow(Op.INSERT, 2, "Bob")));
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

    private void createMockTable(Connection conn, String tableName) throws SQLException {
        Statement stmt = conn.createStatement();
        try {
            stmt.execute("DROP TABLE " + tableName);
        } catch (SQLException e) {
            // Ignored. Derby does not offer "create if not exists" semantics
        }

        try {
            stmt.execute("create table " + tableName + " (id int, name varchar(255))");
            conn.commit();
        } catch (SQLException e) {
            throw io.grpc.Status.INTERNAL.withCause(e).asRuntimeException();
        } finally {
            stmt.close();
        }
    }

    @Test
    public void testJDBCWrite() throws SQLException {
        Connection conn = DriverManager.getConnection(connectionURL);
        JDBCSink sink = new JDBCSink(conn, TableSchema.getMockTableSchema(), "test");
        createMockTable(conn, sink.getTableName());

        sink.write(
                Iterators.forArray(
                        new ArraySinkrow(Op.INSERT, 1, "Alice"),
                        new ArraySinkrow(Op.INSERT, 2, "Bob"),
                        new ArraySinkrow(Op.UPDATE_DELETE, 1, "Alice"),
                        new ArraySinkrow(Op.UPDATE_INSERT, 1, "Clare"),
                        new ArraySinkrow(Op.DELETE, 2, "Bob")));
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

    @Test
    public void testJDBCDrop() throws SQLException {
        Connection conn = DriverManager.getConnection(connectionURL);
        JDBCSink sink = new JDBCSink(conn, TableSchema.getMockTableSchema(), "test");
        sink.drop();
        try {
            assertTrue(conn.isClosed());
        } catch (SQLException e) {
            fail(String.valueOf(e));
        }
    }
}
