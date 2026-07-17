package io.tapdata.risingwave;

import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Adapts TapData updates to complete RisingWave rows. Relational images must reconstruct the row;
 * after-only {@code _id} events require TapData full-document filling to be explicitly enabled.
 */
final class RisingWaveCdcNormalizer {
    private RisingWaveCdcNormalizer() {
    }

    static Update normalizeUpdate(TapTable table, TapUpdateRecordEvent event) {
        Map<String, TapField> fields = requireSchema(table);
        Map<String, Object> after = event.getAfter();
        if (after == null) {
            throw new IllegalArgumentException("CDC update for table " + tableName(table)
                    + " is missing its after image");
        }
        validateColumns(table, after);

        List<String> primaryKeys = primaryKeys(table);
        boolean mongoAfterOnly = primaryKeys.size() == 1
                && "_id".equals(primaryKeys.get(0))
                && (event.getBefore() == null || event.getBefore().isEmpty());
        boolean authoritative = mongoAfterOnly || Boolean.TRUE.equals(event.getIsReplaceEvent());

        LinkedHashMap<String, Object> values = new LinkedHashMap<>();
        if (!authoritative && event.getBefore() != null) {
            for (String field : fields.keySet()) {
                if (event.getBefore().containsKey(field)) {
                    values.put(field, event.getBefore().get(field));
                }
            }
        }
        values.putAll(after);
        applyRemovedFields(table, fields.keySet(), after, values, event.getRemovedFields());

        LinkedHashMap<String, Object> rowAfter = new LinkedHashMap<>();
        Set<String> missing = new LinkedHashSet<>();
        for (String field : fields.keySet()) {
            if (values.containsKey(field)) {
                rowAfter.put(field, values.get(field));
            } else if (authoritative) {
                rowAfter.put(field, null);
            } else {
                missing.add(field);
            }
        }
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException("CDC update for table " + tableName(table)
                    + " cannot form a complete post-image; missing columns " + missing);
        }

        Map<String, Object> newIdentity = identity(rowAfter, primaryKeys);
        if (!primaryKeys.isEmpty() && newIdentity == null) {
            throw new IllegalArgumentException("CDC update for table " + tableName(table)
                    + " is missing new primary-key values " + primaryKeys);
        }

        Map<String, Object> oldFilter;
        if (primaryKeys.isEmpty()) {
            oldFilter = completeKeylessRecord(table, event.getBefore(), "update");
        } else {
            oldFilter = identity(event.getBefore(), primaryKeys);
            if (oldFilter == null && mongoAfterOnly) {
                oldFilter = newIdentity;
            }
            if (oldFilter == null) {
                throw new IllegalArgumentException("CDC update for table " + tableName(table)
                        + " is missing old primary-key values " + primaryKeys);
            }
        }

        return new Update(rowAfter, oldFilter,
                !primaryKeys.isEmpty()
                        && !sameIdentity(fields, primaryKeys, oldFilter, newIdentity));
    }

    static Map<String, Object> deleteFilter(TapTable table, Map<String, Object> before) {
        List<String> primaryKeys = primaryKeys(table);
        if (primaryKeys.isEmpty()) {
            return completeKeylessRecord(table, before, "delete");
        }
        Map<String, Object> identity = identity(before, primaryKeys);
        if (identity == null) {
            throw new IllegalArgumentException("CDC delete for table " + tableName(table)
                    + " is missing primary-key values " + primaryKeys);
        }
        return identity;
    }

    static void validateColumns(TapTable table, Map<String, Object> record) {
        Map<String, TapField> fields = requireSchema(table);
        if (record == null) {
            throw new IllegalArgumentException("CDC record for table " + tableName(table)
                    + " is missing its row image");
        }
        for (String field : record.keySet()) {
            if (!fields.containsKey(field)) {
                throw unknownField(table, field);
            }
        }
    }

    static List<String> primaryKeys(TapTable table) {
        if (table == null || table.getNameFieldMap() == null) {
            return Collections.emptyList();
        }
        List<TapField> fields = new ArrayList<>();
        for (TapField field : table.getNameFieldMap().values()) {
            if (Boolean.TRUE.equals(field.getPrimaryKey())
                    || (field.getPrimaryKeyPos() != null && field.getPrimaryKeyPos() > 0)) {
                fields.add(field);
            }
        }
        fields.sort(Comparator.comparing(
                TapField::getPrimaryKeyPos,
                Comparator.nullsLast(Comparator.naturalOrder())));
        List<String> primaryKeys = new ArrayList<>(fields.size());
        for (TapField field : fields) {
            primaryKeys.add(field.getName());
        }
        return primaryKeys;
    }

    private static void applyRemovedFields(
            TapTable table,
            Set<String> fields,
            Map<String, Object> after,
            Map<String, Object> values,
            List<String> removedFields) {
        if (removedFields == null) {
            return;
        }
        for (String removed : removedFields) {
            if (removed == null || removed.isEmpty()) {
                continue;
            }
            if (fields.contains(removed)) {
                values.put(removed, null);
                continue;
            }
            int dot = removed.indexOf('.');
            String parent = dot > 0 ? removed.substring(0, dot) : null;
            if (parent == null || !fields.contains(parent)) {
                throw unknownField(table, removed);
            }
            if (!after.containsKey(parent)) {
                throw new IllegalArgumentException("CDC update for table " + tableName(table)
                        + " removes nested field " + removed
                        + " without a complete post-image for column " + parent);
            }
        }
    }

    private static Map<String, Object> completeKeylessRecord(
            TapTable table, Map<String, Object> record, String operation) {
        validateColumns(table, record);
        Set<String> missing = new LinkedHashSet<>(table.getNameFieldMap().keySet());
        missing.removeAll(record.keySet());
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException("CDC " + operation + " for keyless table "
                    + tableName(table) + " requires a complete before image; missing columns " + missing);
        }
        LinkedHashMap<String, Object> complete = new LinkedHashMap<>();
        for (String field : table.getNameFieldMap().keySet()) {
            complete.put(field, record.get(field));
        }
        return complete;
    }

    private static Map<String, Object> identity(
            Map<String, Object> record, List<String> primaryKeys) {
        if (primaryKeys.isEmpty()) {
            return Collections.emptyMap();
        }
        if (record == null || !record.keySet().containsAll(primaryKeys)) {
            return null;
        }
        LinkedHashMap<String, Object> identity = new LinkedHashMap<>();
        for (String primaryKey : primaryKeys) {
            Object value = record.get(primaryKey);
            if (value == null) {
                return null;
            }
            identity.put(primaryKey, value);
        }
        return identity;
    }

    private static boolean sameIdentity(
            Map<String, TapField> fields,
            List<String> primaryKeys,
            Map<String, Object> left,
            Map<String, Object> right) {
        for (String primaryKey : primaryKeys) {
            Object leftValue = left.get(primaryKey);
            Object rightValue = right.get(primaryKey);
            String type = RisingWaveSql.canonicalType(fields.get(primaryKey).getDataType());
            boolean numeric = "smallint".equals(type) || "integer".equals(type)
                    || "bigint".equals(type) || "numeric".equals(type)
                    || "real".equals(type) || "double precision".equals(type);
            boolean equal = Objects.deepEquals(leftValue, rightValue);
            if (numeric && leftValue instanceof Number && rightValue instanceof Number) {
                try {
                    equal = new BigDecimal(leftValue.toString())
                            .compareTo(new BigDecimal(rightValue.toString())) == 0;
                } catch (NumberFormatException ignored) {
                    // Fall through to the Java representation.
                }
            }
            if (!equal) {
                return false;
            }
        }
        return true;
    }

    private static Map<String, TapField> requireSchema(TapTable table) {
        if (table == null || table.getNameFieldMap() == null || table.getNameFieldMap().isEmpty()) {
            throw new IllegalArgumentException("CDC operation for table " + tableName(table)
                    + " requires a complete table schema");
        }
        return table.getNameFieldMap();
    }

    private static IllegalArgumentException unknownField(TapTable table, String field) {
        return new IllegalArgumentException("CDC record for table " + tableName(table)
                + " contains unknown field " + field
                + "; automatic schema evolution is not supported");
    }

    private static String tableName(TapTable table) {
        return table == null || table.getId() == null ? "<unknown>" : table.getId();
    }

    static final class Update {
        final Map<String, Object> rowAfter;
        final Map<String, Object> oldFilter;
        final boolean keyChanged;

        private Update(
                Map<String, Object> rowAfter,
                Map<String, Object> oldFilter,
                boolean keyChanged) {
            this.rowAfter = rowAfter;
            this.oldFilter = oldFilter;
            this.keyChanged = keyChanged;
        }
    }
}
