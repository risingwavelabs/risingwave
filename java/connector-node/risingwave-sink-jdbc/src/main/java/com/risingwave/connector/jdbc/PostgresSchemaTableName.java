package com.risingwave.connector.jdbc;

public class PostgresSchemaTableName extends SchemaTableName {
    public PostgresSchemaTableName(String schemaName, String tableName) {
        super(schemaName, tableName);

        if (schemaName == null || schemaName.isBlank()) {
            this.schemaName = "public";
        }
    }
}
