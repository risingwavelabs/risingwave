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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class RedShiftDialect extends PostgresDialect {
    private final List<Integer> columnSqlTypes;
    private final List<Integer> pkIndices;

    public RedShiftDialect(List<Integer> columnSqlTypes, List<Integer> pkIndices) {
        super(columnSqlTypes, pkIndices);
        this.columnSqlTypes = columnSqlTypes;
        this.pkIndices = pkIndices;
    }

    @Override
    public Optional<String> getUpsertStatement(
            SchemaTableName schemaTableName,
            TableSchema tableSchema,
            List<String> primaryKeyFields) {
        List<String> fieldNames = List.of(tableSchema.getColumnNames());
        var columnDescs = tableSchema.getColumnDescs();
        var tableName = getNormalizedTableName(schemaTableName);
        // Build the VALUES placeholders for the source data
        String valuesPlaceholders =
                columnDescs.stream()
                        .map(
                                column -> {
                                    System.out.println(
                                            "column = " + column.getDataType().getTypeName());
                                    String jdbcName =
                                            RW_TYPE_TO_JDBC_TYPE_NAME.get(
                                                    column.getDataType().getTypeName());
                                    return "CAST(? AS "
                                            + jdbcName
                                            + ") AS "
                                            + quoteIdentifier(column.getName());
                                })
                        .collect(Collectors.joining(", "));
        // Build the ON condition for primary key matching
        String onCondition =
                primaryKeyFields.stream()
                        .map(
                                pk ->
                                        tableName
                                                + "."
                                                + quoteIdentifier(pk)
                                                + " = source."
                                                + quoteIdentifier(pk))
                        .collect(Collectors.joining(" AND "));

        // Build the UPDATE SET clause
        String updateClause =
                fieldNames.stream()
                        .map(f -> quoteIdentifier(f) + " = source." + quoteIdentifier(f))
                        .collect(Collectors.joining(", "));

        // Build the INSERT columns and values
        String insertColumns =
                fieldNames.stream().map(this::quoteIdentifier).collect(Collectors.joining(", "));
        String insertValues =
                fieldNames.stream()
                        .map(f -> "source." + quoteIdentifier(f))
                        .collect(Collectors.joining(", "));

        return Optional.of(
                "MERGE INTO "
                        + tableName
                        + " USING (SELECT "
                        + valuesPlaceholders
                        + ") AS source"
                        + " ON "
                        + onCondition
                        + " WHEN MATCHED THEN UPDATE SET "
                        + updateClause
                        + " WHEN NOT MATCHED THEN INSERT ("
                        + insertColumns
                        + ") VALUES ("
                        + insertValues
                        + ")");
    }

    @Override
    public void set_jsonb(int placeholderIdx, int columnIdx, PreparedStatement stmt, SinkRow row)
            throws SQLException {
        stmt.setString(placeholderIdx, (String) row.get(columnIdx));
    }
}
