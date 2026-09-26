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
import static org.junit.Assert.assertTrue;

import com.risingwave.connector.source.common.JniOracleExternalTable;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.Data.DataType.TypeName;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class OracleExternalTableTest extends OracleSourceTestBase {
    // Oracle can briefly reject flashback reads after creating the test table.
    private static final Duration FLASHBACK_READ_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration FLASHBACK_RETRY_INTERVAL = Duration.ofMillis(250);

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
    public void discoversOracleTypesUsingDebeziumCompatibleWidths() throws Exception {
        createTable(
                "APP.EXT_TYPES",
                "CREATE TABLE APP.EXT_TYPES ("
                        + "ID NUMBER(9) PRIMARY KEY, "
                        + "N4 NUMBER(4,0), "
                        + "N9 NUMBER(9,0), "
                        + "N18 NUMBER(18,0), "
                        + "N19 NUMBER(19,0), "
                        + "N7_NEG2 NUMBER(7,-2), "
                        + "N_UNBOUNDED NUMBER, "
                        + "N_STAR NUMBER(*,0), "
                        + "N10_2 NUMBER(10,2), "
                        + "BINARY_FLOAT_VALUE BINARY_FLOAT, "
                        + "BINARY_DOUBLE_VALUE BINARY_DOUBLE, "
                        + "FLOAT_VALUE FLOAT, "
                        + "DOUBLE_VALUE DOUBLE PRECISION, "
                        + "VARCHAR_VALUE VARCHAR2(32), "
                        + "RAW_VALUE RAW(32), "
                        + "DATE_VALUE DATE, "
                        + "TIMESTAMP_VALUE TIMESTAMP(6), "
                        + "TIMESTAMPTZ_VALUE TIMESTAMP(6) WITH TIME ZONE, "
                        + "BOOLEAN_VALUE BOOLEAN, "
                        + "JSON_VALUE JSON)");
        createTable(
                "APP.EXT_LONG_TYPE",
                "CREATE TABLE APP.EXT_LONG_TYPE " + "(ID NUMBER(9) PRIMARY KEY, LONG_VALUE LONG)");
        createTable(
                "APP.EXT_LONG_RAW_TYPE",
                "CREATE TABLE APP.EXT_LONG_RAW_TYPE "
                        + "(ID NUMBER(9) PRIMARY KEY, LONG_RAW_VALUE LONG RAW)");

        var expectedTypes = new LinkedHashMap<String, TypeName>();
        expectedTypes.put("ID", TypeName.INT32);
        expectedTypes.put("N4", TypeName.INT16);
        expectedTypes.put("N9", TypeName.INT32);
        expectedTypes.put("N18", TypeName.INT64);
        expectedTypes.put("N19", TypeName.DECIMAL);
        expectedTypes.put("N7_NEG2", TypeName.INT32);
        expectedTypes.put("N_UNBOUNDED", TypeName.DECIMAL);
        expectedTypes.put("N_STAR", TypeName.DECIMAL);
        expectedTypes.put("N10_2", TypeName.DECIMAL);
        expectedTypes.put("BINARY_FLOAT_VALUE", TypeName.FLOAT);
        expectedTypes.put("BINARY_DOUBLE_VALUE", TypeName.DOUBLE);
        expectedTypes.put("FLOAT_VALUE", TypeName.DECIMAL);
        expectedTypes.put("DOUBLE_VALUE", TypeName.DECIMAL);
        expectedTypes.put("VARCHAR_VALUE", TypeName.VARCHAR);
        expectedTypes.put("RAW_VALUE", TypeName.BYTEA);
        expectedTypes.put("DATE_VALUE", TypeName.TIMESTAMP);
        expectedTypes.put("TIMESTAMP_VALUE", TypeName.TIMESTAMP);
        expectedTypes.put("TIMESTAMPTZ_VALUE", TypeName.TIMESTAMPTZ);
        expectedTypes.put("BOOLEAN_VALUE", TypeName.BOOLEAN);
        expectedTypes.put("JSON_VALUE", TypeName.JSONB);

        assertDiscoveredTypes("EXT_TYPES", expectedTypes);
        assertDiscoveredTypes(
                "EXT_LONG_TYPE", Map.of("ID", TypeName.INT32, "LONG_VALUE", TypeName.VARCHAR));
        assertDiscoveredTypes(
                "EXT_LONG_RAW_TYPE",
                Map.of("ID", TypeName.INT32, "LONG_RAW_VALUE", TypeName.BYTEA));
    }

    @Test
    public void rejectsUnsupportedIntervalDiscoveredFromOracle() throws Exception {
        createTable(
                "APP.EXT_INTERVAL_TYPE",
                "CREATE TABLE APP.EXT_INTERVAL_TYPE "
                        + "(ID NUMBER(9) PRIMARY KEY, INTERVAL_VALUE INTERVAL DAY TO SECOND)");

        assertDiscoveryError("EXT_INTERVAL_TYPE", "Unsupported Oracle data type: INTERVAL");
    }

    @Test
    public void readsFirstAndSubsequentCompositePrimaryKeyPagesAtFixedScn() throws Exception {
        var qualifiedTable = "APP.EXT_SNAPSHOT";
        createTable(
                qualifiedTable,
                "CREATE TABLE "
                        + qualifiedTable
                        + " (REGION VARCHAR2(16), ID NUMBER(9), TEST_VALUE VARCHAR2(32), "
                        + "ACTIVE BOOLEAN, PRIMARY KEY (REGION, ID))");
        execute(
                "INSERT ALL "
                        + "INTO "
                        + qualifiedTable
                        + " VALUES ('east', 1, 'east-one', TRUE) "
                        + "INTO "
                        + qualifiedTable
                        + " VALUES ('east', 2, 'east-two', FALSE) "
                        + "INTO "
                        + qualifiedTable
                        + " VALUES ('east', 3, 'east-three', TRUE) "
                        + "INTO "
                        + qualifiedTable
                        + " VALUES ('west', 1, 'west-one', FALSE) "
                        + "INTO "
                        + qualifiedTable
                        + " VALUES ('west', 2, 'west-two', TRUE) SELECT 1 FROM DUAL");

        var catalogRequest = catalogRequest("EXT_SNAPSHOT");
        var tableSchema = discover(catalogRequest).getTableSchema();
        assertEquals(
                List.of("REGION", "ID", "TEST_VALUE", "ACTIVE"),
                tableSchema.getColumnsList().stream().map(column -> column.getName()).toList());
        assertEquals(List.of(0, 1), tableSchema.getPkIndicesList());

        var snapshotRequest =
                ConnectorServiceProto.OracleExternalTableRequest.newBuilder()
                        .putAllProperties(catalogRequest.getPropertiesMap())
                        .setTableSchema(tableSchema)
                        .addAllPrimaryKeys(List.of("REGION", "ID"))
                        .setLimit(2);
        var firstPage = readFirstAvailableSnapshot(catalogRequest, snapshotRequest);
        var snapshotScn = snapshotRequest.getSnapshotScn();
        assertTrue(snapshotScn > 0);
        var fixedSnapshotRequest = snapshotRequest.build();

        execute("INSERT INTO " + qualifiedTable + " VALUES ('west', 3, 'after-snapshot', TRUE)");
        assertEquals(6, queryInt("SELECT COUNT(*) FROM " + qualifiedTable));
        assertEquals(snapshotScn, fixedSnapshotRequest.getSnapshotScn());

        var rows = new ArrayList<String>();
        var pageSizes = new ArrayList<Integer>();
        var page = firstPage;
        for (int pageIndex = 0; pageIndex < 4; pageIndex++) {
            pageSizes.add(page.getRowsCount());
            if (page.getRowsCount() == 0) {
                break;
            }
            for (var row : page.getRowsList()) {
                rows.add(
                        datumText(row.getValues(0))
                                + ":"
                                + datumText(row.getValues(1))
                                + "="
                                + datumText(row.getValues(2))
                                + ":"
                                + datumText(row.getValues(3)));
            }
            var lastRow = page.getRows(page.getRowsCount() - 1);
            var pageRequest =
                    fixedSnapshotRequest.toBuilder()
                            .clearStartPk()
                            .addStartPk(lastRow.getValues(0))
                            .addStartPk(lastRow.getValues(1))
                            .build();
            page = snapshotRead(pageRequest);
            assertEquals(snapshotScn, pageRequest.getSnapshotScn());
        }

        assertEquals(List.of(2, 2, 1, 0), pageSizes);
        assertEquals(
                List.of(
                        "east:1=east-one:true",
                        "east:2=east-two:false",
                        "east:3=east-three:true",
                        "west:1=west-one:false",
                        "west:2=west-two:true"),
                rows);
    }

    @Test
    public void rejectsNegativeSnapshotScnAndLimit() throws Exception {
        var validRequest =
                ConnectorServiceProto.OracleExternalTableRequest.newBuilder()
                        .setTableSchema(ConnectorServiceProto.TableSchema.getDefaultInstance())
                        .setSnapshotScn(1)
                        .setLimit(1);

        assertSnapshotError(
                validRequest.clone().setSnapshotScn(-1).build(), "invalid snapshot SCN");
        assertSnapshotError(validRequest.clone().setLimit(-1).build(), "invalid limit");
    }

    private void assertDiscoveredTypes(String tableName, Map<String, TypeName> expectedTypes)
            throws Exception {
        var tableSchema = discover(catalogRequest(tableName)).getTableSchema();
        var actualTypes = new LinkedHashMap<String, TypeName>();
        for (var column : tableSchema.getColumnsList()) {
            actualTypes.put(column.getName(), column.getColumnType().getTypeName());
        }
        assertEquals(expectedTypes, actualTypes);
        assertEquals(List.of(0), tableSchema.getPkIndicesList());
    }

    private void assertDiscoveryError(String tableName, String expectedMessage) throws Exception {
        var response =
                parseResponse(
                        JniOracleExternalTable.discover(catalogRequest(tableName).toByteArray()));
        assertTrue(
                response.getError().getErrorMessage(),
                response.getError().getErrorMessage().contains(expectedMessage));
    }

    private void assertSnapshotError(
            ConnectorServiceProto.OracleExternalTableRequest request, String expectedMessage)
            throws Exception {
        var response = snapshotReadResponse(request);
        assertTrue(
                response.getError().getErrorMessage(),
                response.getError().getErrorMessage().contains(expectedMessage));
    }

    private ConnectorServiceProto.OracleExternalTableRequest catalogRequest(String tableName) {
        return ConnectorServiceProto.OracleExternalTableRequest.newBuilder()
                .putAllProperties(oracle.sourceProperties(tableName))
                .build();
    }

    private ConnectorServiceProto.OracleExternalTableResponse discover(
            ConnectorServiceProto.OracleExternalTableRequest request) throws Exception {
        return successfulResponse(JniOracleExternalTable.discover(request.toByteArray()));
    }

    private ConnectorServiceProto.OracleExternalTableResponse snapshotRead(
            ConnectorServiceProto.OracleExternalTableRequest request) throws Exception {
        return successfulResponse(snapshotReadResponse(request));
    }

    private ConnectorServiceProto.OracleExternalTableResponse snapshotReadResponse(
            ConnectorServiceProto.OracleExternalTableRequest request) throws Exception {
        return parseResponse(JniOracleExternalTable.snapshotRead(request.toByteArray()));
    }

    private ConnectorServiceProto.OracleExternalTableResponse readFirstAvailableSnapshot(
            ConnectorServiceProto.OracleExternalTableRequest catalogRequest,
            ConnectorServiceProto.OracleExternalTableRequest.Builder snapshotRequest)
            throws Exception {
        var deadline = System.nanoTime() + FLASHBACK_READ_TIMEOUT.toNanos();
        var attempts = 0;
        while (true) {
            attempts++;
            var currentScn =
                    successfulResponse(
                                    JniOracleExternalTable.currentScn(catalogRequest.toByteArray()))
                            .getSnapshotScn();
            snapshotRequest.setSnapshotScn(currentScn);
            var response = snapshotReadResponse(snapshotRequest.build());
            var error = response.getError().getErrorMessage();
            if (error.isEmpty()) {
                return response;
            }
            assertTrue(error, error.contains("ORA-01466"));
            if (System.nanoTime() >= deadline) {
                throw new AssertionError(
                        String.format(
                                "Oracle table '%s.%s' did not become available for flashback reads "
                                        + "within %d seconds after %d attempts; last SCN %d: %s",
                                catalogRequest.getPropertiesOrDefault("schema.name", "<unknown>"),
                                catalogRequest.getPropertiesOrDefault("table.name", "<unknown>"),
                                FLASHBACK_READ_TIMEOUT.toSeconds(),
                                attempts,
                                currentScn,
                                error));
            }
            Thread.sleep(FLASHBACK_RETRY_INTERVAL.toMillis());
        }
    }

    private static ConnectorServiceProto.OracleExternalTableResponse successfulResponse(
            byte[] bytes) throws Exception {
        return successfulResponse(parseResponse(bytes));
    }

    private static ConnectorServiceProto.OracleExternalTableResponse successfulResponse(
            ConnectorServiceProto.OracleExternalTableResponse response) {
        assertEquals("", response.getError().getErrorMessage());
        return response;
    }

    private static ConnectorServiceProto.OracleExternalTableResponse parseResponse(byte[] bytes)
            throws Exception {
        return ConnectorServiceProto.OracleExternalTableResponse.parseFrom(bytes);
    }

    private static String datumText(ConnectorServiceProto.OracleDatum datum) {
        return datum.getValue().toString(StandardCharsets.UTF_8);
    }
}
