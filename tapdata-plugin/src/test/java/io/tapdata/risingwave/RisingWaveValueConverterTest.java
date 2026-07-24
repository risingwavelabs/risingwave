package io.tapdata.risingwave;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.value.DateTime;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PGobject;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.OffsetDateTime;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RisingWaveValueConverterTest {

    @Test
    void preservesExactNumbersInJsonbDocuments() {
        TapTable table = new TapTable("events")
                .add(new TapField("decimal_value", "numeric"))
                .add(new TapField("integer_value", "numeric"));
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("decimal_value", new BigDecimal("1234567890.1234567890"));
        record.put("integer_value", new BigInteger("123456789012345678901234567890"));

        Map<String, Object> normalized = RisingWaveValueConverter.normalizeStreamingRecord(
                record, table, false);

        assertEquals("1234567890.1234567890", normalized.get("decimal_value"));
        assertEquals("123456789012345678901234567890", normalized.get("integer_value"));
    }

    @Test
    void convertsTypedBinaryValuesToPostgresByteaHex() {
        TapTable table = new TapTable("events").add(new TapField("payload", "bytea"));
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("payload", new byte[]{0, 15, (byte) 255});

        Map<String, Object> normalized = RisingWaveValueConverter.normalizeStreamingRecord(
                record, table, true);

        assertEquals("\\x000fff", normalized.get("payload"));
    }

    @Test
    void normalizesNestedCollectionsWithoutChangingTheirShape() {
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("values", Arrays.asList(new BigDecimal("1.25"), new BigInteger("9")));
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("document", nested);

        Map<String, Object> normalized = RisingWaveValueConverter.normalizeStreamingRecord(
                record, null, false);

        Map<?, ?> normalizedNested = (Map<?, ?>) normalized.get("document");
        assertEquals(Arrays.asList("1.25", "9"), normalizedNested.get("values"));
    }

    @Test
    void bindsJsonbAsAJsonbPgObject() throws Exception {
        TapField field = new TapField("attributes", "jsonb");
        Map<String, Object> attributes = new LinkedHashMap<>();
        attributes.put("name", "TapData");
        attributes.put("count", 2);

        Object value = RisingWaveValueConverter.toJdbcValue(attributes, field);

        assertTrue(value instanceof PGobject);
        PGobject jsonb = (PGobject) value;
        assertEquals("jsonb", jsonb.getType());
        assertEquals("{\"name\":\"TapData\",\"count\":2}", jsonb.getValue());
    }

    @Test
    void preservesOffsetDateTimeUntilTransportConversion() throws Exception {
        DateTime dateTime = new DateTime(ZonedDateTime.of(
                2026, 7, 16, 10, 11, 12, 123_000_000,
                ZoneOffset.ofHoursMinutes(5, 30)));
        TapField field = new TapField("created_at", "timestamp with time zone")
                .tapType(new TapDateTime().withTimeZone(true));
        TapTable table = new TapTable("events").add(field);

        Map<String, Object> normalized = RisingWaveValueConverter.normalizeStreamingRecord(
                java.util.Collections.singletonMap("created_at", dateTime), table, true);
        Object jdbc = RisingWaveValueConverter.toJdbcValue(dateTime, field);

        assertEquals("2026-07-16T10:11:12.123+05:30", normalized.get("created_at"));
        assertEquals(OffsetDateTime.parse("2026-07-16T10:11:12.123+05:30"), jdbc);
    }

    @Test
    void omitsOffsetForPlainTimestampTargets() throws Exception {
        DateTime dateTime = new DateTime(ZonedDateTime.of(
                2026, 7, 16, 10, 11, 12, 123_000_000,
                ZoneOffset.ofHoursMinutes(5, 30)));
        TapField field = new TapField("created_at", "timestamp without time zone")
                .tapType(new TapDateTime().withTimeZone(false));
        TapTable table = new TapTable("events").add(field);

        Map<String, Object> normalized = RisingWaveValueConverter.normalizeStreamingRecord(
                java.util.Collections.singletonMap("created_at", dateTime), table, true);
        Object jdbc = RisingWaveValueConverter.toJdbcValue(dateTime, field);

        assertEquals("2026-07-16T10:11:12.123", normalized.get("created_at"));
        assertEquals(LocalDateTime.parse("2026-07-16T10:11:12.123"), jdbc);
    }
}
