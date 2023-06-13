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

import static java.lang.String.format;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** A dialect for a specific database. ref: https://github.com/apache/flink-connector-jdbc */
public interface JdbcDialect {

    /**
     * Quotes the identifier.
     *
     * <p>Used to put quotes around the identifier if the column name is a reserved keyword or
     * contains characters requiring quotes (e.g., space).
     *
     * @return the quoted identifier.
     */
    String quoteIdentifier(String identifier);

    /**
     * Constructs the dialects upsert statement if supported; such as MySQL's {@code DUPLICATE KEY
     * UPDATE}, or PostgreSQL's {@code ON CONFLICT... DO UPDATE SET..}. If supported, the returned
     * string will be used as a {@link java.sql.PreparedStatement}. Fields in the statement must be
     * in the same order as the {@code fieldNames} parameter.
     *
     * <p>If the dialect does not support native upsert statements, the writer will fallback to
     * {@code SELECT} + {@code UPDATE}/{@code INSERT} which may have poor performance.
     *
     * @return The upsert statement if supported, otherwise None.
     */
    Optional<String> getUpsertStatement(
            String tableName, List<String> fieldNames, List<String> uniqueKeyFields);

    /**
     * Generates a string that will be used as a {@link java.sql.PreparedStatement} to insert a row
     * into a database table. Fields in the statement must be in the same order as the {@code
     * fieldNames} parameter.
     *
     * @return the dialects {@code INSERT INTO} statement.
     */
    default String getInsertIntoStatement(String tableName, List<String> fieldNames) {
        String columns =
                fieldNames.stream().map(this::quoteIdentifier).collect(Collectors.joining(", "));
        String placeholders = fieldNames.stream().map(f -> "?").collect(Collectors.joining(", "));
        return "INSERT INTO "
                + quoteIdentifier(tableName)
                + "("
                + columns
                + ")"
                + " VALUES ("
                + placeholders
                + ")";
    }

    /**
     * A simple single row {@code DELETE} statement.
     *
     * <pre>{@code
     * DELETE FROM table_name
     * WHERE cond [AND ...]
     * }</pre>
     */
    default String getDeleteStatement(String tableName, List<String> conditionFields) {
        String conditionClause =
                conditionFields.stream()
                        .map(f -> format("%s = ?", quoteIdentifier(f)))
                        .collect(Collectors.joining(" AND "));
        return "DELETE FROM " + quoteIdentifier(tableName) + " WHERE " + conditionClause;
    }
}
