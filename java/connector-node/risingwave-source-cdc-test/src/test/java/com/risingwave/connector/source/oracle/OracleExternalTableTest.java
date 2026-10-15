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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.risingwave.connector.source.common.JniOracleExternalTable;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.Data.DataType.TypeName;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
    public void rejectsPrimaryKeysWithInexactOrdering() throws Exception {
        var unsupportedTypes = new LinkedHashMap<String, String>();
        unsupportedTypes.put("NUMBER", "requires a declared NUMBER precision");
        unsupportedTypes.put("NUMBER(29)", "requires a declared NUMBER precision");
        unsupportedTypes.put("NUMBER(28,29)", "requires a declared NUMBER precision");
        unsupportedTypes.put("NUMBER(28,-1)", "requires a declared NUMBER precision");
        unsupportedTypes.put("TIMESTAMP(9)", "unsupported type TIMESTAMP");
        unsupportedTypes.put("NVARCHAR2(20)", "unsupported type NVARCHAR2");
        unsupportedTypes.put("NCHAR(20)", "unsupported type NCHAR");
        var index = 0;
        for (var entry : unsupportedTypes.entrySet()) {
            var table = "EXT_PK_UNSUPPORTED_" + index++;
            createTable(
                    "APP." + table,
                    "CREATE TABLE APP." + table + " (ID " + entry.getKey() + " PRIMARY KEY)");
            assertDiscoveryError(table, entry.getValue());
        }
    }

    @Test
    public void acceptsLosslessPrimaryKeyTypes() throws Exception {
        var supportedTypes = new LinkedHashMap<String, TypeName>();
        supportedTypes.put("NUMBER(28,0)", TypeName.DECIMAL);
        supportedTypes.put("NUMBER(28,28)", TypeName.DECIMAL);
        supportedTypes.put("NUMBER(27,-1)", TypeName.DECIMAL);
        supportedTypes.put("RAW(16)", TypeName.BYTEA);
        supportedTypes.put("DATE", TypeName.TIMESTAMP);
        supportedTypes.put("TIMESTAMP(6)", TypeName.TIMESTAMP);
        supportedTypes.put("VARCHAR2(20)", TypeName.VARCHAR);
        supportedTypes.put("CHAR(20)", TypeName.VARCHAR);
        var index = 0;
        for (var entry : supportedTypes.entrySet()) {
            var table = "EXT_PK_SUPPORTED_" + index++;
            createTable(
                    "APP." + table,
                    "CREATE TABLE APP." + table + " (ID " + entry.getKey() + " PRIMARY KEY)");
            assertDiscoveredTypes(table, Map.of("ID", entry.getValue()));
        }
    }

    @Test
    public void discoversCompositePrimaryKey() throws Exception {
        var qualifiedTable = "APP.EXT_COMPOSITE_PK";
        createTable(
                qualifiedTable,
                "CREATE TABLE "
                        + qualifiedTable
                        + " (REGION VARCHAR2(16), ID NUMBER(9), TEST_VALUE VARCHAR2(32), "
                        + "ACTIVE BOOLEAN, PRIMARY KEY (REGION, ID))");

        var tableSchema = discover(connectionProperties("EXT_COMPOSITE_PK"));
        assertEquals(
                List.of("REGION", "ID", "TEST_VALUE", "ACTIVE"),
                tableSchema.getColumnsList().stream().map(column -> column.getName()).toList());
        assertEquals(List.of(0, 1), tableSchema.getPkIndicesList());
    }

    private void assertDiscoveredTypes(String tableName, Map<String, TypeName> expectedTypes)
            throws Exception {
        var tableSchema = discover(connectionProperties(tableName));
        var actualTypes = new LinkedHashMap<String, TypeName>();
        for (var column : tableSchema.getColumnsList()) {
            actualTypes.put(column.getName(), column.getColumnType().getTypeName());
        }
        assertEquals(expectedTypes, actualTypes);
        assertEquals(List.of(0), tableSchema.getPkIndicesList());
    }

    private void assertDiscoveryError(String tableName, String expectedMessage) {
        var properties = connectionProperties(tableName);
        var error =
                assertThrows(
                        RuntimeException.class,
                        () -> JniOracleExternalTable.discover(properties.toByteArray()));
        assertTrue(error.getMessage(), error.getMessage().contains(expectedMessage));
    }

    private ConnectorServiceProto.OracleConnectionProperties connectionProperties(
            String tableName) {
        return ConnectorServiceProto.OracleConnectionProperties.newBuilder()
                .putAllProperties(oracle.sourceProperties(tableName))
                .build();
    }

    private ConnectorServiceProto.TableSchema discover(
            ConnectorServiceProto.OracleConnectionProperties properties) throws Exception {
        return ConnectorServiceProto.TableSchema.parseFrom(
                JniOracleExternalTable.discover(properties.toByteArray()));
    }
}
