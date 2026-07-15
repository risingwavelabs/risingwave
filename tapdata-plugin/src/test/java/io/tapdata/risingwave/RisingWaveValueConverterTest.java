package io.tapdata.risingwave;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
