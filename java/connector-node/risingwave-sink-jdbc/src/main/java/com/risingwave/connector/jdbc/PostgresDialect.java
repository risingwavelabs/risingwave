// Copyright 2025 RisingWave Labs
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

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.proto.Data.DataType.TypeName;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGobject;

public class PostgresDialect implements JdbcDialect {

    private final int[] columnSqlTypes;
    private final int[] pkIndices;

    public PostgresDialect(List<Integer> columnSqlTypes, List<Integer> pkIndices) {
        this.columnSqlTypes = columnSqlTypes.stream().mapToInt(i -> i).toArray();
        this.pkIndices = pkIndices.stream().mapToInt(i -> i).toArray();
    }

    private static final HashMap<TypeName, String> RW_TYPE_TO_JDBC_TYPE_NAME;

    static {
        RW_TYPE_TO_JDBC_TYPE_NAME = new HashMap<TypeName, String>();
        RW_TYPE_TO_JDBC_TYPE_NAME.put(TypeName.INT16, "int2");
        RW_TYPE_TO_JDBC_TYPE_NAME.put(TypeName.INT32, "int4");
        RW_TYPE_TO_JDBC_TYPE_NAME.put(TypeName.INT64, "int8");
        RW_TYPE_TO_JDBC_TYPE_NAME.put(TypeName.FLOAT, "float4");
        RW_TYPE_TO_JDBC_TYPE_NAME.put(TypeName.DOUBLE, "float8");
        RW_TYPE_TO_JDBC_TYPE_NAME.put(TypeName.BOOLEAN, "bool");
        RW_TYPE_TO_JDBC_TYPE_NAME.put(TypeName.VARCHAR, "varchar");
        RW_TYPE_TO_JDBC_TYPE_NAME.put(TypeName.DECIMAL, "numeric");
        RW_TYPE_TO_JDBC_TYPE_NAME.put(TypeName.TIME, "time");
        RW_TYPE_TO_JDBC_TYPE_NAME.put(TypeName.TIMESTAMP, "timestamp");
        RW_TYPE_TO_JDBC_TYPE_NAME.put(TypeName.INTERVAL, "varchar");
        RW_TYPE_TO_JDBC_TYPE_NAME.put(TypeName.DATE, "date");
        RW_TYPE_TO_JDBC_TYPE_NAME.put(TypeName.TIMESTAMPTZ, "timestamptz");
        RW_TYPE_TO_JDBC_TYPE_NAME.put(TypeName.JSONB, "varchar");
    }

    @Override
    public SchemaTableName createSchemaTableName(String schemaName, String tableName) {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public String getNormalizedTableName(SchemaTableName schemaTableName) {
        if (schemaTableName.schemaName == null || schemaTableName.schemaName.isBlank()) {
            return quoteIdentifier(schemaTableName.getTableName());
        } else {
            return quoteIdentifier(schemaTableName.getSchemaName())
                    + '.'
                    + quoteIdentifier(schemaTableName.getTableName());
        }
    }

    @Override
    public String quoteIdentifier(String identifier) {
        // quote identifier will be case-sensitive in postgres
        return "\"" + identifier + "\"";
    }

    @Override
    public Optional<String> getUpsertStatement(
            SchemaTableName schemaTableName,
            List<String> fieldNames,
            List<String> primaryKeyFields) {
        String pkColumns =
                primaryKeyFields.stream()
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String updateClause =
                fieldNames.stream()
                        .map(f -> quoteIdentifier(f) + "=EXCLUDED." + quoteIdentifier(f))
                        .collect(Collectors.joining(", "));
        return Optional.of(
                getInsertIntoStatement(schemaTableName, fieldNames)
                        + " ON CONFLICT ("
                        + pkColumns
                        + ")"
                        + " DO UPDATE SET "
                        + updateClause);
    }

    @Override
    public void bindUpsertStatement(
            PreparedStatement stmt, Connection conn, TableSchema tableSchema, SinkRow row)
            throws SQLException {
        bindInsertIntoStatement(stmt, conn, tableSchema, row);
    }

    @Override
    public void bindInsertIntoStatement(
            PreparedStatement stmt, Connection conn, TableSchema tableSchema, SinkRow row)
            throws SQLException {
        var columnDescs = tableSchema.getColumnDescs();
        int placeholderIdx = 1;
        for (int columnIdx = 0; columnIdx < row.size(); columnIdx++) {
            var column = columnDescs.get(columnIdx);
            switch (column.getDataType().getTypeName()) {
                case DECIMAL:
                    stmt.setBigDecimal(placeholderIdx++, (java.math.BigDecimal) row.get(columnIdx));
                    break;
                case INTERVAL:
                    stmt.setObject(placeholderIdx++, new PGInterval((String) row.get(columnIdx)));
                    break;
                case JSONB:
                    // reference: https://github.com/pgjdbc/pgjdbc/issues/265
                    var pgObj = new PGobject();
                    pgObj.setType("jsonb");
                    pgObj.setValue((String) row.get(columnIdx));
                    stmt.setObject(placeholderIdx++, pgObj);
                    break;
                case BYTEA:
                    stmt.setBytes(placeholderIdx++, (byte[]) row.get(columnIdx));
                    break;
                case LIST:
                    var val = row.get(columnIdx);
                    assert (val instanceof Object[]);
                    Object[] objArray = (Object[]) val;
                    assert (column.getDataType().getFieldTypeCount() == 1);
                    var fieldType = column.getDataType().getFieldType(0);
                    var typeName = RW_TYPE_TO_JDBC_TYPE_NAME.get(fieldType.getTypeName());
                    if (typeName == null) {
                        typeName = fieldType.getTypeName().name();
                    }
                    stmt.setArray(placeholderIdx++, conn.createArrayOf(typeName, objArray));
                    break;
                case VARCHAR:
                    // since VARCHAR column may sink to a UUID column, we get the target type
                    // from the mapping which should be Types.OTHER.
                    stmt.setObject(placeholderIdx++, row.get(columnIdx), columnSqlTypes[columnIdx]);
                    break;
                default:
                    stmt.setObject(placeholderIdx++, row.get(columnIdx));
                    break;
            }
        }
    }

    @Override
    public void bindDeleteStatement(PreparedStatement stmt, TableSchema tableSchema, SinkRow row)
            throws SQLException {
        // set the values of primary key fields
        int placeholderIdx = 1;
        for (int pkIdx : pkIndices) {
            Object pkField = row.get(pkIdx);
            stmt.setObject(placeholderIdx++, pkField, columnSqlTypes[pkIdx]);
        }
    }
}
