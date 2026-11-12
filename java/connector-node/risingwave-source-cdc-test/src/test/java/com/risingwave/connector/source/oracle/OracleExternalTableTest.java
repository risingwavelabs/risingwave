/*
 * Copyright 2026 RisingWave Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.risingwave.connector.source.oracle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.risingwave.connector.source.common.JniOracleExternalTable;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.Data;
import com.risingwave.proto.PlanCommon;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import oracle.ucp.jdbc.PoolDataSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class OracleExternalTableTest extends OracleSourceTestBase {
    private static OracleTestFixture oracle;

    @BeforeClass
    public static void startServices() throws Exception {
        oracle = new OracleTestFixture();
        oracle.start();
    }

    @AfterClass
    public static void stopServices() throws Exception {
        if (oracle != null) {
            oracle.close();
        }
    }

    @Override
    protected OracleTestFixture oracle() {
        return oracle;
    }

    @Test
    public void cancelledSnapshotReadReturnsItsConnectionToThePool() throws Exception {
        createTable("APP.SLOW_SOURCE", "CREATE TABLE APP.SLOW_SOURCE (ID NUMBER(9) PRIMARY KEY)");
        execute("INSERT INTO APP.SLOW_SOURCE VALUES (1)");
        execute(
                "CREATE OR REPLACE FUNCTION APP.SLOW_VALUE(V NUMBER) RETURN NUMBER IS "
                        + "BEGIN DBMS_SESSION.SLEEP(60); RETURN V; END;");
        execute(
                "CREATE OR REPLACE VIEW APP.SLOW_ROWS AS "
                        + "SELECT ID, APP.SLOW_VALUE(ID) AS SLOW_ID FROM APP.SLOW_SOURCE");

        var properties =
                ConnectorServiceProto.OracleConnectionProperties.newBuilder()
                        .putAllProperties(oracle.sourceProperties("SLOW_ROWS"))
                        .build();
        var pool = (PoolDataSource) JniOracleExternalTable.openPool(properties.toByteArray());
        try {
            var cancel =
                    JniOracleExternalTable.newStatementCancel("APP.SLOW_ROWS", "snapshot read");
            var request = slowRowsRequest().toByteArray();
            var read =
                    CompletableFuture.runAsync(
                            () -> JniOracleExternalTable.snapshotRead(pool, request, 0, 0, cancel));
            waitForActiveConnectorQuery();
            assertEquals(1, pool.getBorrowedConnectionsCount());

            JniOracleExternalTable.cancelStatement(cancel);
            read.get(10, TimeUnit.SECONDS);
            assertEquals(0, pool.getBorrowedConnectionsCount());
        } finally {
            JniOracleExternalTable.closePool(pool);
        }
    }

    @Test
    public void snapshotQueryGeneratesPrimaryKeyIndexScanPlan() throws Exception {
        createTable(
                "APP.SNAPSHOT_PLAN",
                "CREATE TABLE APP.SNAPSHOT_PLAN (A NUMBER(9), B NUMBER(9), C NUMBER(9), "
                        + "PAYLOAD VARCHAR2(100), PRIMARY KEY (A, B, C))");
        try (var statement = pdb.createStatement()) {
            statement.execute(
                    "INSERT /*+ APPEND */ INTO APP.SNAPSHOT_PLAN "
                            + "SELECT MOD(n, 10), MOD(TRUNC(n / 10), 100), n, RPAD('x', 100, 'x') "
                            + "FROM (SELECT (x.l - 1) * 1000 + y.l AS n "
                            + "FROM (SELECT LEVEL l FROM DUAL CONNECT BY LEVEL <= 300) x, "
                            + "(SELECT LEVEL l FROM DUAL CONNECT BY LEVEL <= 1000) y)");
            statement.execute("BEGIN DBMS_STATS.GATHER_TABLE_STATS('APP', 'SNAPSHOT_PLAN'); END;");
            statement.execute("ALTER SESSION SET STATISTICS_LEVEL = ALL");
        }

        var primaryKeys = List.of("A", "B", "C");
        var nearStart = firstKeyOfGroup(0, 0);
        var nearEnd = firstKeyOfGroup(8, 98);

        var firstRead = runSnapshotQuery(snapshotSql(primaryKeys, 0), List.of());
        assertReadsThroughPrimaryKeyIndex(firstRead, "INDEX FULL SCAN");

        for (int boundCount = primaryKeys.size(); boundCount >= 1; boundCount--) {
            var sql = snapshotSql(primaryKeys, boundCount);
            var start = runSnapshotQuery(sql, nearStart.subList(0, boundCount));
            var end = runSnapshotQuery(sql, nearEnd.subList(0, boundCount));
            assertReadsThroughPrimaryKeyIndex(start, "INDEX RANGE SCAN");
            assertReadsThroughPrimaryKeyIndex(end, "INDEX RANGE SCAN");
            assertTrue(
                    String.format(
                            "reading near the end costs %d buffer gets but near the start %d: %s",
                            end.bufferGets(), start.bufferGets(), sql),
                    end.bufferGets() <= start.bufferGets() * 3 / 2);
        }
    }

    private record SnapshotQueryRun(int rows, List<String> operations, long bufferGets) {}

    private List<Integer> firstKeyOfGroup(int a, int b) throws SQLException {
        try (var statement =
                pdb.prepareStatement(
                        "SELECT MIN(C) FROM APP.SNAPSHOT_PLAN WHERE A = ? AND B = ?")) {
            statement.setInt(1, a);
            statement.setInt(2, b);
            try (var result = statement.executeQuery()) {
                assertTrue(result.next());
                return List.of(a, b, result.getInt(1));
            }
        }
    }

    private static String snapshotSql(List<String> primaryKeys, int boundCount) throws Exception {
        var columns =
                new ArrayList<PlanCommon.ColumnDesc>(
                        primaryKeys.stream()
                                .map(name -> columnDesc(name, Data.DataType.TypeName.INT32))
                                .toList());
        columns.add(columnDesc("PAYLOAD", Data.DataType.TypeName.VARCHAR));
        var builder =
                Class.forName("com.risingwave.connector.source.common.OracleExternalTable")
                        .getDeclaredMethod(
                                "buildSnapshotSql",
                                List.class,
                                String.class,
                                String.class,
                                List.class,
                                int.class);
        builder.setAccessible(true);
        return (String)
                builder.invoke(null, columns, "APP", "SNAPSHOT_PLAN", primaryKeys, boundCount);
    }

    private static PlanCommon.ColumnDesc columnDesc(String name, Data.DataType.TypeName type) {
        return PlanCommon.ColumnDesc.newBuilder()
                .setName(name)
                .setColumnType(Data.DataType.newBuilder().setTypeName(type))
                .build();
    }

    private SnapshotQueryRun runSnapshotQuery(String sql, List<Integer> bounds)
            throws SQLException {
        var rows = 0;
        try (var statement = pdb.prepareStatement(sql)) {
            statement.setFetchSize(256);
            for (int i = 0; i < bounds.size(); i++) {
                statement.setInt(i + 1, bounds.get(i));
            }
            try (var result = statement.executeQuery()) {
                while (rows < 256 && result.next()) {
                    rows++;
                }
            }
        }
        String sqlId;
        int childNumber;
        try (var statement = pdb.createStatement();
                var result =
                        statement.executeQuery(
                                "SELECT PREV_SQL_ID, PREV_CHILD_NUMBER FROM V$SESSION "
                                        + "WHERE SID = SYS_CONTEXT('USERENV', 'SID')")) {
            assertTrue(result.next());
            sqlId = result.getString(1);
            childNumber = result.getInt(2);
        }
        var operations = new ArrayList<String>();
        var bufferGets = 0L;
        try (var statement =
                pdb.prepareStatement(
                        "SELECT OPERATION || ' ' || OPTIONS, LAST_CR_BUFFER_GETS "
                                + "FROM V$SQL_PLAN_STATISTICS_ALL "
                                + "WHERE SQL_ID = ? AND CHILD_NUMBER = ? ORDER BY ID")) {
            statement.setString(1, sqlId);
            statement.setInt(2, childNumber);
            try (var result = statement.executeQuery()) {
                while (result.next()) {
                    operations.add(result.getString(1));
                    bufferGets = Math.max(bufferGets, result.getLong(2));
                }
            }
        }
        return new SnapshotQueryRun(rows, operations, bufferGets);
    }

    private static void assertReadsThroughPrimaryKeyIndex(
            SnapshotQueryRun run, String expectedIndexAccess) {
        var plan = String.join(" > ", run.operations());
        assertEquals(plan, 256, run.rows());
        assertTrue(plan, run.operations().contains(expectedIndexAccess));
        assertFalse(plan, run.operations().contains("SORT ORDER BY"));
        assertFalse(plan, run.operations().contains("TABLE ACCESS FULL"));
    }

    private static ConnectorServiceProto.OracleSnapshotReadRequest slowRowsRequest() {
        var int32 = Data.DataType.newBuilder().setTypeName(Data.DataType.TypeName.INT32);
        return ConnectorServiceProto.OracleSnapshotReadRequest.newBuilder()
                .setSchemaName(OracleTestFixture.SOURCE_SCHEMA)
                .setTableName("SLOW_ROWS")
                .setTableSchema(
                        ConnectorServiceProto.TableSchema.newBuilder()
                                .addColumns(
                                        PlanCommon.ColumnDesc.newBuilder()
                                                .setName("ID")
                                                .setColumnType(int32))
                                .addColumns(
                                        PlanCommon.ColumnDesc.newBuilder()
                                                .setName("SLOW_ID")
                                                .setColumnType(int32))
                                .addPkIndices(0))
                .addPrimaryKeys("ID")
                .build();
    }

    private void waitForActiveConnectorQuery() throws Exception {
        var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        try (var cdb = oracle.connectAsSystem(OracleTestFixture.CDB);
                var statement =
                        cdb.prepareStatement(
                                "SELECT COUNT(*) FROM V$SESSION s JOIN V$SQL q "
                                        + "ON q.SQL_ID = s.SQL_ID AND q.CHILD_NUMBER = s.SQL_CHILD_NUMBER "
                                        + "WHERE s.USERNAME = ? AND s.STATUS = 'ACTIVE' "
                                        + "AND q.SQL_TEXT LIKE '%\"SLOW_ROWS\"%'")) {
            statement.setString(1, OracleTestFixture.CONNECTOR_USER);
            while (System.nanoTime() < deadline) {
                try (var result = statement.executeQuery()) {
                    if (result.next() && result.getInt(1) > 0) {
                        return;
                    }
                }
                Thread.sleep(100);
            }
        }
        fail("The Oracle snapshot query did not start within 30 seconds");
    }
}
