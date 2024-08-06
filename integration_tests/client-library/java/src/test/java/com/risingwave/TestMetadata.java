package com.risingwave;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TestMetadata {

    public List<String> fetchAllStrings(ResultSet resultSet, String columnName) throws SQLException {
        List<String> stringColumn = new ArrayList<>();

        // Iterate through the ResultSet and collect schema names
        while (resultSet.next()) {
            String element = resultSet.getString(columnName);
            stringColumn.add(element);
        }

        return stringColumn;
    }

    @Test
    public void testGetSchemas() throws SQLException {
        try (Connection connection = TestUtils.establishConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet schemas = metaData.getSchemas()) {
                // Check that the ResultSet is not null
                assertNotNull(schemas);

                List<String> schemaList = fetchAllStrings(schemas, "TABLE_SCHEM");

                // Check that the schema list is not empty
                assertFalse(schemaList.isEmpty());

                // Check that "pg_catalog" schema exists in the list
                assertTrue(schemaList.contains("pg_catalog"));
            }
        }
    }

    @Test
    public void testGetCatalogs() throws SQLException {
        try (Connection connection = TestUtils.establishConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet catalogs = metaData.getCatalogs()) {
                // Check that the ResultSet is not null
                assertNotNull(catalogs);

                List<String> catalogList = fetchAllStrings(catalogs, "TABLE_CAT");

                // Check that the catalog list is not empty
                assertFalse(catalogList.isEmpty());

                // Check that a must-have database "dev" exists in the list
                assertTrue(catalogList.contains("dev"));
            }
        }
    }

    /*
     TODO: Support the parameter 'default_transaction_isolation'.
     @Test
     public void testGetDefaultTransactionIsolation() throws SQLException {
         try (Connection connection = TestUtils.establishConnection()) {
             DatabaseMetaData metaData = connection.getMetaData();
             metaData.getDefaultTransactionIsolation();
         }
     }
    */

    /*
    TODO: Support the parameter 'max_index_keys'.
    @Test
    public void testGetMaxColumnsInIndex() throws SQLException {
        try (Connection connection = TestUtils.establishConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            metaData.getMaxColumnsInIndex();
            metaData.getImportedKeys("dev", "information_schema", "tables");
            metaData.getExportedKeys("dev", "information_schema", "tables");
            metaData.getCrossReference("dev", "information_schema", "tables", "dev", "pg_catalog", "pg_tables");
        }
    }
    */

    /*
    TODO: Support the `name` type and give it a type length.
    @Test
    public void testGetMaxNameLength() throws SQLException {
        try (Connection connection = TestUtils.establishConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            metaData.getMaxCursorNameLength();
            metaData.getMaxSchemaNameLength();
            metaData.getMaxTableNameLength();
            metaData.getMaxSchemaNameLength();
            metaData.getMaxUserNameLength();
            metaData.getMaxProcedureNameLength();
            metaData.getMaxCatalogNameLength();
            metaData.getClientInfoProperties();
        }
    }
    */

    @Test
    public void testGetProcedures() throws SQLException {
        try (Connection connection = TestUtils.establishConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet procedures = metaData.getProcedures("dev", "public", "")) {
                assertNotNull(procedures);
                List<String> procedureList = fetchAllStrings(procedures, "PROCEDURE_NAME");
                assertTrue(procedureList.isEmpty());
            }
        }
    }

    @Test
    public void testGetTables() throws SQLException {
        try (Connection connection = TestUtils.establishConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet tables = metaData.getTables("dev", "pg_catalog", "", null)) {
                assertNotNull(tables);
                List<String> tablesList = fetchAllStrings(tables, "TABLE_NAME");
                assertFalse(tablesList.isEmpty());
            }

            /*
            TODO: `relacl` is missing in `pg_class`.
            try (ResultSet acl = metaData.getTablePrivileges("dev", "information_schema", "tables")) {
                assertNotNull(acl);
            }
             */
        }
    }

    @Test
    public void testGetColumns() throws SQLException {
        try (Connection connection = TestUtils.establishConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet columns = metaData.getColumns("dev", "information_schema", "tables", "")) {
                assertNotNull(columns);
                List<String> columnList = fetchAllStrings(columns, "COLUMN_NAME");
                assertFalse(columnList.isEmpty());
            }

            /*
            TODO: `relacl` is missing in `pg_class`.
            try (ResultSet acl = metaData.getColumnPrivileges("dev", "information_schema", "tables", "")) {
                assertNotNull(acl);
            }
             */
        }
    }

    /*
    TODO: the query used in getPrimaryKeys retrieves nothing in RisingWave.
    @Test
    public void testGetPrimaryKeys() throws SQLException {
        try (Connection connection = TestUtils.establishConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();

            TestUtils.executeDdl(connection, "CREATE TABLE t_test_get_pks (id INT PRIMARY KEY, value TEXT);");

            try (ResultSet pKeys = metaData.getPrimaryKeys("dev", "public", "t_test_get_pks")) {
                assertNotNull(pKeys);
                List<String> pKeyList = fetchAllStrings(pKeys, "PK_NAME");
                assertFalse(pKeyList.isEmpty());
            }

            try (ResultSet pKeys = metaData.getPrimaryUniqueKeys("dev", "public", "t_test_get_pks")) {
                assertNotNull(pKeys);
                List<String> pKeyList = fetchAllStrings(pKeys, "PK_NAME");
                assertFalse(pKeyList.isEmpty());
            }

            TestUtils.executeDdl(connection, "DROP TABLE t_test_get_pks;");
        }
    }
    */


    /*
    TODO: Support pg_get_function_result.
    TODO: prokind is missing in pg_proc.
    @Test
    public void testGetFunctions() throws SQLException {
        try (Connection connection = TestUtils.establishConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();

            try (ResultSet functions = metaData.getFunctions("dev", "public", "")) {
                assertNotNull(functions);
                List<String> fList = fetchAllStrings(functions, "FUNCTION_NAME");
                assertFalse(fList.isEmpty());
            }

            try (ResultSet cols = metaData.getFunctionColumns("dev", "public", "")) {
                assertNotNull(cols);
            }
        }
    }
    */

    @Test
    public void testGetUDTs() throws SQLException {
        try (Connection connection = TestUtils.establishConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();

            try (ResultSet types = metaData.getUDTs("dev", "public", "", null)) {
                assertNotNull(types);
            }
        }
    }

    /**
     * Only checks the API callability, not results.
     */
    @Test
    public void testApiCallable() throws SQLException {
        try (Connection connection = TestUtils.establishConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();

            metaData.getTableTypes();
            metaData.getTypeInfo();

            /// metaData.getIndexInfo();

            // TODO: Actually implement `pg_catalog.pg_get_keywords()`. Now it returns empty.
            metaData.getSQLKeywords();

            metaData.getBestRowIdentifier("dev", "information_schema", "tables", 1, true);
            metaData.getVersionColumns("dev", "information_schema", "tables");
        }
    }
}
