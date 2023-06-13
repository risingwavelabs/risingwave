package com.risingwave.connector.jdbc;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class PostgresDialect implements JdbcDialect {

    @Override
    public String quoteIdentifier(String identifier) {
        return identifier;
    }

    /**
     * INSERT INTO t1 VALUES (1, 8.8, 'AA') on conflict (v1) DO UPDATE SET
     * v1=EXCLUDED.v1,v2=EXCLUDED.v2,v3=EXCLUDED.v3;
     *
     * <p>prepared statement: INSERT INTO t1 (field1, field2, field3) VALUES (?, ?, ?) on conflict
     * (pk_field1, pk_field2) DO UPDATE SET
     * field1=EXCLUDED.field1,field2=EXCLUDED.field2,field3=EXCLUDED.field3;
     */
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
}
