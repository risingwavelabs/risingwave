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

import com.risingwave.proto.Data;

public abstract class JdbcUtils {

    public static String getDbSqlType(
            DatabaseType db, Data.DataType.TypeName typeName, Object value) {
        String sqlType;
        switch (db) {
            case MYSQL:
                sqlType = getMySqlSqlType(typeName, value);
                break;
            case POSTGRES:
                sqlType = getPostgresSqlType(typeName, value);
                break;
            default:
                throw io.grpc.Status.INVALID_ARGUMENT
                        .withDescription("Unsupported database type: " + db)
                        .asRuntimeException();
        }
        return sqlType;
    }

    // https://dev.mysql.com/doc/workbench/en/wb-migration-database-postgresql-typemapping.html
    private static String getMySqlSqlType(Data.DataType.TypeName typeName, Object value) {
        // convert to mysql data type name from postgres data type
        switch (typeName) {
            case INT32:
                return "INT";
            case INT64:
                return "BIGINT";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case BOOLEAN:
                return "TINYINT";
            case VARCHAR:
                if (!(value instanceof String)) {
                    throw io.grpc.Status.INVALID_ARGUMENT
                            .withDescription("Expected string, got " + value.getClass())
                            .asRuntimeException();
                }
                String string = (String) value;
                if (string.length() > 65535) {
                    return "LONGTEXT";
                } else {
                    return "VARCHAR";
                }
            case BYTEA:
                return "LONGBLOB";
            case DATE:
                return "DATE";
            case TIME:
            case INTERVAL:
                return "TIME";
            case TIMESTAMP:
                return "TIMESTAMP";
            case DECIMAL:
                return "DECIMAL";
            default:
                return null;
        }
    }

    private static String getPostgresSqlType(Data.DataType.TypeName typeName, Object value) {
        return typeName.name();
    }
}
