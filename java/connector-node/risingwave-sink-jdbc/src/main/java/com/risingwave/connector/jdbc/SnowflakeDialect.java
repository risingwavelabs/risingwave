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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

public class SnowflakeDialect implements JdbcDialect {
    @Override
    public SchemaTableName createSchemaTableName(String schemaName, String tableName) {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public String getNormalizedTableName(SchemaTableName schemaTableName) {
        return schemaTableName.getTableName();
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public Optional<String> getUpsertStatement(
            SchemaTableName schemaTableName,
            TableSchema tableSchema,
            List<String> uniqueKeyFields) {
        throw new UnsupportedOperationException(
                "SnowflakeDialect does not support upsert statements");
    }

    @Override
    public void bindUpsertStatement(
            PreparedStatement stmt, Connection conn, TableSchema tableSchema, SinkRow row)
            throws SQLException {
        throw new UnsupportedOperationException(
                "SnowflakeDialect does not support upsert statements");
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
                case JSONB:
                    stmt.setObject(placeholderIdx++, row.get(columnIdx));
                    break;
                case BYTEA:
                    stmt.setBytes(placeholderIdx++, (byte[]) row.get(columnIdx));
                    break;
                case VARCHAR:
                    stmt.setString(placeholderIdx++, (String) row.get(columnIdx));
                    break;
                case DATE:
                    java.time.LocalDate date = (java.time.LocalDate) row.get(columnIdx);
                    stmt.setDate(
                            placeholderIdx++, date != null ? java.sql.Date.valueOf(date) : null);
                    break;
                case TIME:
                    java.time.LocalTime time = (java.time.LocalTime) row.get(columnIdx);
                    stmt.setTime(
                            placeholderIdx++, time != null ? java.sql.Time.valueOf(time) : null);
                    break;
                case TIMESTAMP:
                    java.time.LocalDateTime datetime = (java.time.LocalDateTime) row.get(columnIdx);
                    stmt.setTimestamp(
                            placeholderIdx++,
                            datetime != null ? java.sql.Timestamp.valueOf(datetime) : null);
                    break;
                case TIMESTAMPTZ:
                    java.time.OffsetDateTime offsetDateTime =
                            (java.time.OffsetDateTime) row.get(columnIdx);
                    String isoTimestamp =
                            offsetDateTime != null ? (offsetDateTime).toString() : null;
                    stmt.setString(placeholderIdx++, isoTimestamp);
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
        throw new UnsupportedOperationException(
                "SnowflakeDialect does not support DELETE statements");
    }
}
