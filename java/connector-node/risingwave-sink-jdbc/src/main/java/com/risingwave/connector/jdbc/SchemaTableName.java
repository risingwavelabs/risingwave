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

package com.risingwave.connector.jdbc;

/**
 * A simple class to hold the schema name and table name. A database dialect may extend this class
 * to customize the schema name and table name for the database. And the getNormalizedTableName()
 * should generate a normalized table name for the database.
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
