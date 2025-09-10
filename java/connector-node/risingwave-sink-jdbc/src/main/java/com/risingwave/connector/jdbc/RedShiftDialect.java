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
        throw new UnsupportedOperationException(
                "RedShiftDialect does not support upsert statements");
    }

    @Override
    public void bindUpsertStatement(
            PreparedStatement stmt, Connection conn, TableSchema tableSchema, SinkRow row)
            throws SQLException {
        throw new UnsupportedOperationException(
                "RedShiftDialect does not support upsert statements");
    }

    @Override
    public void bindDeleteStatement(PreparedStatement stmt, TableSchema tableSchema, SinkRow row)
            throws SQLException {
        throw new UnsupportedOperationException(
                "RedShiftDialect does not support DELETE statements");
    }

    @Override
    public void set_jsonb(int placeholderIdx, int columnIdx, PreparedStatement stmt, SinkRow row)
            throws SQLException {
        stmt.setString(placeholderIdx, (String) row.get(columnIdx));
    }
}
