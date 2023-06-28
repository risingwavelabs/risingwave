package com.risingwave.connector.jdbc;

/**
 * A simple class to hold the schema name and table name. A database dialect may extend this class
 * and override the getNormalizedTableName() method to generate a qualified table name to be used in
 * SQL statements.
 */
public class SchemaTableName {
    protected String schemaName;
    protected String tableName;

    public SchemaTableName(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getNormalizedTableName() {
        if (schemaName != null && !schemaName.isBlank()) {
            return schemaName + '.' + tableName;
        } else {
            return tableName;
        }
    }
}
