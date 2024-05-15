// Copyright 2024 RisingWave Labs
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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySqlDialect implements JdbcDialect {

    private final int[] pkIndices;
    private final int[] pkColumnSqlTypes;
    static final Logger LOG = LoggerFactory.getLogger(MySqlDialect.class);

    public MySqlDialect(List<Integer> columnSqlTypes, List<Integer> pkIndices) {
        var columnSqlTypesArr = columnSqlTypes.stream().mapToInt(i -> i).toArray();
        this.pkIndices = pkIndices.stream().mapToInt(i -> i).toArray();

        // derive sql types for pk columns
        var pkColumnSqlTypes = new int[pkIndices.size()];
        for (int i = 0; i < pkIndices.size(); i++) {
            pkColumnSqlTypes[i] = columnSqlTypesArr[this.pkIndices[i]];
        }
        this.pkColumnSqlTypes = pkColumnSqlTypes;
    }

    @Override
    public SchemaTableName createSchemaTableName(String schemaName, String tableName) {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public String getNormalizedTableName(SchemaTableName schemaTableName) {
        return quoteIdentifier(schemaTableName.getTableName());
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public Optional<String> getUpsertStatement(
            SchemaTableName schemaTableName,
            List<String> fieldNames,
            List<String> uniqueKeyFields) {
        String updateClause =
                fieldNames.stream()
                        .map(f -> quoteIdentifier(f) + "=VALUES(" + quoteIdentifier(f) + ")")
                        .collect(Collectors.joining(", "));
        return Optional.of(
                getInsertIntoStatement(schemaTableName, fieldNames)
                        + " ON DUPLICATE KEY UPDATE "
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
        for (int i = 0; i < row.size(); i++) {
            var column = columnDescs.get(i);
            switch (column.getDataType().getTypeName()) {
                case DECIMAL:
                    stmt.setBigDecimal(placeholderIdx++, (java.math.BigDecimal) row.get(i));
                    break;
                case INTERVAL:
                case JSONB:
                    stmt.setObject(placeholderIdx++, row.get(i));
                    break;
                case BYTEA:
                    stmt.setBytes(placeholderIdx++, (byte[]) row.get(i));
                    break;
                case LIST:
                    var val = row.get(i);
                    assert (val instanceof Object[]);
                    Object[] objArray = (Object[]) val;
                    // convert Array type to a string for other database
                    // reference:
                    // https://dev.mysql.com/doc/workbench/en/wb-migration-database-postgresql-typemapping.html
                    var arrayString = StringUtils.join(objArray, ",");
                    stmt.setString(placeholderIdx++, arrayString);
                    break;
                case STRUCT:
                    throw new RuntimeException("STRUCT type is not supported yet");
                default:
                    stmt.setObject(placeholderIdx++, row.get(i));
                    break;
            }
        }
    }

    @Override
    public void bindDeleteStatement(PreparedStatement stmt, TableSchema tableSchema, SinkRow row)
            throws SQLException {
        // set the values of primary key fields
        int placeholderIdx = 1;
        var columnDescs = tableSchema.getColumnDescs();
        for (int i = 0; i < pkIndices.length; ++i) {
            int pkIdx = pkIndices[i];
            Object pkField = row.get(pkIdx);
            // remove the milliseconds part from the timestamp/timestamptz
            switch (columnDescs.get(pkIdx).getDataType().getTypeName()) {
                case TIMESTAMP:
                    LocalDateTime ldt = (LocalDateTime) pkField;
                    pkField =
                            LocalDateTime.ofEpochSecond(
                                    ldt.toEpochSecond(ZoneOffset.UTC), 0, ZoneOffset.UTC);
                    break;
                case TIMESTAMPTZ:
                    OffsetDateTime odt = (OffsetDateTime) pkField;
                    pkField = LocalDateTime.ofEpochSecond(odt.toEpochSecond(), 0, ZoneOffset.UTC);
                    break;
                default:
                    break;
            }
            stmt.setObject(placeholderIdx++, pkField, pkColumnSqlTypes[i]);
        }
    }
}
