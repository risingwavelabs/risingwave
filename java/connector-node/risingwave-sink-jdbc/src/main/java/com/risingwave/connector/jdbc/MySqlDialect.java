package com.risingwave.connector.jdbc;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class MySqlDialect implements JdbcDialect {

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public Optional<String> getUpsertStatement(
            String tableName, List<String> fieldNames, List<String> uniqueKeyFields) {
        String updateClause =
                fieldNames.stream()
                        .map(f -> quoteIdentifier(f) + "=VALUES(" + quoteIdentifier(f) + ")")
                        .collect(Collectors.joining(", "));
        return Optional.of(
                getInsertIntoStatement(tableName, fieldNames)
                        + " ON DUPLICATE KEY UPDATE "
                        + updateClause);
    }
}
