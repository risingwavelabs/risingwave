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

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkRow;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGobject;

public class PostgresDialect implements JdbcDialect {

    @Override
    public String quoteIdentifier(String identifier) {
        return identifier;
    }

    @Override
    public Optional<String> getUpsertStatement(
            String tableName, List<String> fieldNames, List<String> primaryKeyFields) {
        String pkColumns =
                primaryKeyFields.stream()
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String updateClause =
                fieldNames.stream()
                        .map(f -> quoteIdentifier(f) + "=EXCLUDED." + quoteIdentifier(f))
                        .collect(Collectors.joining(", "));
        return Optional.of(
                getInsertIntoStatement(tableName, fieldNames)
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
        var columnDescs = tableSchema.getColumnDescs();
        int placeholderIdx = 1;
        for (int i = 0; i < row.size(); i++) {
            var column = columnDescs.get(i);
            switch (column.getDataType().getTypeName()) {
                case DECIMAL:
                    stmt.setBigDecimal(placeholderIdx++, (java.math.BigDecimal) row.get(i));
                    break;
                case INTERVAL:
                    stmt.setObject(placeholderIdx++, new PGInterval((String) row.get(i)));
                    break;
                case JSONB:
                    // reference: https://github.com/pgjdbc/pgjdbc/issues/265
                    var pgObj = new PGobject();
                    pgObj.setType("jsonb");
                    pgObj.setValue((String) row.get(i));
                    stmt.setObject(placeholderIdx++, pgObj);
                    break;
                case BYTEA:
                    stmt.setBytes(placeholderIdx++, (byte[]) row.get(i));
                    break;
                case LIST:
                    var val = row.get(i);
                    assert (val instanceof Object[]);
                    Object[] objArray = (Object[]) val;
                    assert (column.getDataType().getFieldTypeCount() == 1);
                    var fieldType = column.getDataType().getFieldType(0);
                    stmt.setArray(
                            i + 1, conn.createArrayOf(fieldType.getTypeName().name(), objArray));
                    break;
                default:
                    stmt.setObject(placeholderIdx++, row.get(i));
                    break;
            }
        }
    }
}
