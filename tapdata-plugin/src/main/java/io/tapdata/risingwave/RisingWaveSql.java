package io.tapdata.risingwave;

import io.tapdata.entity.schema.TapField;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

/** SQL identifier and RisingWave type helpers shared by discovery, DDL, and DML paths. */
final class RisingWaveSql {
    private RisingWaveSql() {
    }

    static String quoteIdentifier(String identifier) {
        Objects.requireNonNull(identifier, "identifier");
        return '"' + identifier.replace("\"", "\"\"") + '"';
    }

    static String buildTableFilter(List<String> tables) {
        if (tables == null || tables.isEmpty()) {
            return "";
        }
        StringBuilder filter = new StringBuilder();
        for (int index = 0; index < tables.size(); index++) {
            if (index > 0) {
                filter.append(',');
            }
            filter.append('\'').append(tables.get(index).replace("'", "''")).append('\'');
        }
        return filter.toString();
    }

    static String quoteColumns(List<String> columns) {
        StringBuilder sql = new StringBuilder();
        for (int index = 0; index < columns.size(); index++) {
            if (index > 0) {
                sql.append(", ");
            }
            sql.append(quoteIdentifier(columns.get(index)));
        }
        return sql.toString();
    }

    static String placeholders(int count) {
        StringBuilder sql = new StringBuilder();
        for (int index = 0; index < count; index++) {
            if (index > 0) {
                sql.append(", ");
            }
            sql.append('?');
        }
        return sql.toString();
    }

    static String webhookValidationClause(String webhookSecret, String jsonbPayloadColumn) {
        if (webhookSecret == null || webhookSecret.isEmpty()) {
            return "";
        }
        String escapedSecret = webhookSecret.replace("'", "''");
        String signedPayload = jsonbPayloadColumn == null
                ? "payload" : quoteIdentifier(jsonbPayloadColumn);
        return " VALIDATE AS secure_compare("
                + "headers->>'x-rw-signature', "
                + "'sha256=' || encode(hmac('" + escapedSecret
                + "', " + signedPayload + ", 'sha256'), 'hex'))";
    }

    static String mapDiscoveredType(String dataType, String udtName) {
        if (dataType == null) {
            return "text";
        }
        switch (dataType.toLowerCase(Locale.ROOT)) {
            case "integer": case "int": case "int4": return "integer";
            case "bigint": case "int8": return "bigint";
            case "smallint": case "int2": return "smallint";
            case "real": case "float4": return "real";
            case "double precision": case "float8": return "double precision";
            case "numeric": case "decimal": return "numeric";
            case "boolean": case "bool": return "boolean";
            case "character varying": case "varchar": return "varchar";
            case "character": case "char": return "varchar";
            case "text": return "text";
            case "date": return "date";
            case "time without time zone": case "time": return "time";
            case "timestamp without time zone": case "timestamp": return "timestamp";
            case "timestamp with time zone": case "timestamptz": return "timestamp with time zone";
            case "bytea": return "bytea";
            case "jsonb": case "json": return "jsonb";
            case "uuid": return "varchar";
            default:
                return udtName == null || udtName.isEmpty()
                        ? "text"
                        : mapDiscoveredType(udtName, null);
        }
    }

    static String targetType(TapField field) {
        String dataType = field.getDataType();
        if (dataType == null) {
            return "text";
        }
        String lower = dataType.toLowerCase(Locale.ROOT);
        if (lower.startsWith("varchar(") || lower.startsWith("character varying(")) {
            return "varchar";
        }
        if (lower.startsWith("char(") || lower.equals("char")
                || lower.startsWith("character(") || lower.equals("character")) {
            return "varchar";
        }
        if (lower.startsWith("numeric(") || lower.startsWith("decimal(")) {
            return "numeric";
        }
        if (lower.contains("serial")) {
            return "bigint";
        }
        return dataType;
    }

    static String canonicalType(String dataType) {
        if (dataType == null) {
            return "text";
        }
        String type = dataType.trim().toLowerCase(Locale.ROOT).replaceAll("\\s+", " ");
        int parameters = type.indexOf('(');
        if (parameters >= 0) {
            type = type.substring(0, parameters).trim();
        }
        switch (type) {
            case "int": case "int4": case "integer": return "integer";
            case "int8": case "bigint": case "bigserial": case "serial": return "bigint";
            case "int2": case "smallint": return "smallint";
            case "float4": case "real": return "real";
            case "float8": case "double precision": return "double precision";
            case "decimal": case "numeric": return "numeric";
            case "bool": case "boolean": return "boolean";
            case "char": case "character": case "character varying": case "text":
            case "varchar": case "uuid": return "varchar";
            case "time without time zone": case "time": return "time";
            case "timestamp without time zone": case "timestamp": return "timestamp";
            case "timestamptz": case "timestamp with time zone": return "timestamp with time zone";
            case "json": case "jsonb": return "jsonb";
            default: return type;
        }
    }
}
