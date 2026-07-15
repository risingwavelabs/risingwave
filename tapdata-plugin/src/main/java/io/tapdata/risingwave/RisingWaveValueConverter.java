package io.tapdata.risingwave;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapDate;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapJson;
import io.tapdata.entity.schema.type.TapMap;
import io.tapdata.entity.schema.type.TapTime;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.DateTime;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Converts TapData values into JDBC parameters or WebSocket-ingest JSON values. */
final class RisingWaveValueConverter {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private RisingWaveValueConverter() {
    }

    static void setJdbcParameter(PreparedStatement statement, int index, Object value)
            throws SQLException {
        statement.setObject(index, toJdbcValue(value));
    }

    static Map<String, Object> normalizeStreamingRecord(
            Map<String, Object> record, TapTable table, boolean typedColumns) {
        if (record == null) {
            return null;
        }

        LinkedHashMap<String, Object> normalized = new LinkedHashMap<>();
        Map<String, TapField> fields = table == null ? null : table.getNameFieldMap();
        for (Map.Entry<String, Object> entry : record.entrySet()) {
            TapField field = fields == null ? null : fields.get(entry.getKey());
            normalized.put(entry.getKey(), normalizeStreamingValue(
                    entry.getValue(), field, typedColumns, false));
        }
        return normalized;
    }

    private static Object toJdbcValue(Object value) {
        if (value instanceof DateTime) {
            return ((DateTime) value).toTimestamp();
        }
        if (value instanceof java.util.Date && !(value instanceof java.sql.Date)
                && !(value instanceof Timestamp)) {
            return new Timestamp(((java.util.Date) value).getTime());
        }
        return value;
    }

    private static Object normalizeStreamingValue(
            Object value, TapField field, boolean typedColumns, boolean stringifyExactNumbers) {
        if (value == null) {
            return null;
        }

        TapType tapType = field == null ? null : field.getTapType();
        if (tapType instanceof TapDate && value instanceof DateTime) {
            java.sql.Date date = ((DateTime) value).toSqlDate();
            return date == null ? null : date.toString();
        }
        if (tapType instanceof TapTime && value instanceof DateTime) {
            java.sql.Time time = ((DateTime) value).toTime();
            return time == null ? null : time.toString();
        }
        if (tapType instanceof TapDateTime && value instanceof DateTime) {
            DateTime dateTime = (DateTime) value;
            if (dateTime.getTimeZone() != null) {
                ZonedDateTime zonedDateTime = dateTime.toZonedDateTime();
                if (zonedDateTime != null) {
                    return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(
                            zonedDateTime.toOffsetDateTime());
                }
            }
            Timestamp timestamp = dateTime.toTimestamp();
            return timestamp == null ? null : timestamp.toString();
        }

        boolean stringifyNumbers = stringifyExactNumbers || !typedColumns
                || isDecimalField(field) || isJsonLikeField(field, tapType);
        if (stringifyNumbers && value instanceof BigDecimal) {
            // RisingWave's webhook decoder can round JSON numbers through floating point.
            return ((BigDecimal) value).toPlainString();
        }
        if (stringifyNumbers && value instanceof BigInteger) {
            return value.toString();
        }
        if (typedColumns && value instanceof byte[] && isBinaryField(field)) {
            return toPostgresByteaHex((byte[]) value);
        }
        if (isJsonLikeField(field, tapType) && value instanceof CharSequence) {
            String json = value.toString().trim();
            if (json.isEmpty()) {
                return value.toString();
            }
            try {
                return JSON_MAPPER.readValue(json, Object.class);
            } catch (java.io.IOException e) {
                throw new IllegalArgumentException("Invalid JSON value for field "
                        + (field == null ? "<unknown>" : field.getName()), e);
            }
        }

        if (value instanceof Map<?, ?>) {
            LinkedHashMap<String, Object> normalized = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                String key = entry.getKey() == null ? "null" : entry.getKey().toString();
                normalized.put(key, normalizeStreamingValue(
                        entry.getValue(), null, typedColumns, stringifyNumbers));
            }
            return normalized;
        }
        if (value instanceof Collection<?>) {
            List<Object> normalized = new ArrayList<>();
            for (Object element : (Collection<?>) value) {
                normalized.add(normalizeStreamingValue(
                        element, null, typedColumns, stringifyNumbers));
            }
            return normalized;
        }
        if (value.getClass().isArray() && !(value instanceof byte[])) {
            int length = Array.getLength(value);
            List<Object> normalized = new ArrayList<>(length);
            for (int index = 0; index < length; index++) {
                normalized.add(normalizeStreamingValue(
                        Array.get(value, index), null, typedColumns, stringifyNumbers));
            }
            return normalized;
        }
        if (value instanceof DateTime) {
            Timestamp timestamp = ((DateTime) value).toTimestamp();
            return timestamp == null ? null : timestamp.toString();
        }
        return value;
    }

    private static boolean isJsonLikeField(TapField field, TapType tapType) {
        if (tapType instanceof TapJson || tapType instanceof TapMap) {
            return true;
        }
        return field != null && field.getDataType() != null
                && field.getDataType().toLowerCase(Locale.ROOT).contains("json");
    }

    private static boolean isDecimalField(TapField field) {
        if (field == null || field.getDataType() == null) {
            return false;
        }
        String dataType = field.getDataType().toLowerCase(Locale.ROOT);
        return dataType.startsWith("numeric") || dataType.startsWith("decimal");
    }

    private static boolean isBinaryField(TapField field) {
        return field != null && field.getDataType() != null
                && "bytea".equalsIgnoreCase(field.getDataType().trim());
    }

    private static String toPostgresByteaHex(byte[] bytes) {
        char[] hex = new char[2 + bytes.length * 2];
        hex[0] = '\\';
        hex[1] = 'x';
        char[] digits = "0123456789abcdef".toCharArray();
        for (int index = 0; index < bytes.length; index++) {
            int value = bytes[index] & 0xff;
            hex[2 + index * 2] = digits[value >>> 4];
            hex[3 + index * 2] = digits[value & 0x0f];
        }
        return new String(hex);
    }
}
