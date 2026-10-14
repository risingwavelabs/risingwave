/*
 * Copyright 2026 RisingWave Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.risingwave.connector.cdc.debezium.converters;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import java.sql.Types;
import java.util.Arrays;
import java.util.BitSet;
import java.util.OptionalInt;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

public class MySqlBitToBytesConverterTest {

    /** Registers the converter for a column and returns it, or null if nothing was registered. */
    private static CustomConverter.Converter converterFor(String typeName, OptionalInt length) {
        var registered = new CustomConverter.Converter[1];
        new MySqlBitToBytesConverter()
                .converterFor(
                        new FakeColumn(typeName, length),
                        (SchemaBuilder schema, CustomConverter.Converter converter) -> {
                            assertEquals(Schema.Type.BYTES, schema.type());
                            registered[0] = converter;
                        });
        return registered[0];
    }

    private static CustomConverter.Converter bitConverter(int numBits) {
        return converterFor("BIT", OptionalInt.of(numBits));
    }

    private static BitSet bitSet(int... setBits) {
        var bits = new BitSet();
        for (int i : setBits) {
            bits.set(i);
        }
        return bits;
    }

    @Test
    public void testIssueExampleBit10() {
        // b'1010' in BIT(10) is \x000a when read from a MySQL result set.
        var converter = bitConverter(10);
        byte[] expected = {0x00, 0x0a};
        // Binlog path: BitSet with bit 1 and bit 3 set.
        assertArrayEquals(expected, (byte[]) converter.convert(bitSet(1, 3)));
        // JDBC path: big-endian bytes, unchanged.
        assertArrayEquals(expected, (byte[]) converter.convert(new byte[] {0x00, 0x0a}));
        // Column default value path: big-endian bytes without leading zero bytes.
        assertArrayEquals(expected, (byte[]) converter.convert(new byte[] {0x0a}));
    }

    @Test
    public void testBit1IsNotConverted() {
        // BIT(1) and BIT (whose length defaults to 1) stay as Debezium booleans.
        assertNull(bitConverter(1));
        assertNull(converterFor("BIT", OptionalInt.empty()));
    }

    @Test
    public void testOtherTypesAreNotConverted() {
        assertNull(converterFor("BINARY", OptionalInt.of(10)));
        assertNull(converterFor("VARBINARY", OptionalInt.of(10)));
        assertNull(converterFor("BLOB", OptionalInt.empty()));
    }

    @Test
    public void testBit8() {
        var converter = bitConverter(8);
        // b'10000001'
        assertArrayEquals(new byte[] {(byte) 0x81}, (byte[]) converter.convert(bitSet(0, 7)));
        assertArrayEquals(new byte[] {0x00}, (byte[]) converter.convert(new BitSet()));
    }

    @Test
    public void testBit9() {
        var converter = bitConverter(9);
        // b'100000001'
        byte[] expected = {0x01, 0x01};
        assertArrayEquals(expected, (byte[]) converter.convert(bitSet(0, 8)));
        assertArrayEquals(expected, (byte[]) converter.convert(new byte[] {0x01, 0x01}));
        // b'1'
        assertArrayEquals(new byte[] {0x00, 0x01}, (byte[]) converter.convert(bitSet(0)));
    }

    @Test
    public void testBit64() {
        var converter = bitConverter(64);
        // Highest and lowest bits set.
        byte[] expected = {(byte) 0x80, 0, 0, 0, 0, 0, 0, 0x01};
        assertArrayEquals(expected, (byte[]) converter.convert(bitSet(0, 63)));
        assertArrayEquals(expected, (byte[]) converter.convert(expected.clone()));
        // All bits set.
        var allOnes = new BitSet();
        allOnes.set(0, 64);
        byte[] expectedAllOnes = new byte[8];
        Arrays.fill(expectedAllOnes, (byte) 0xff);
        assertArrayEquals(expectedAllOnes, (byte[]) converter.convert(allOnes));
    }

    @Test
    public void testNull() {
        assertNull(bitConverter(10).convert(null));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnexpectedType() {
        bitConverter(10).convert("1010");
    }

    private static class FakeColumn implements RelationalColumn {
        private final String typeName;
        private final OptionalInt length;

        FakeColumn(String typeName, OptionalInt length) {
            this.typeName = typeName;
            this.length = length;
        }

        @Override
        public int jdbcType() {
            return Types.OTHER;
        }

        @Override
        public int nativeType() {
            return 0;
        }

        @Override
        public String typeName() {
            return typeName;
        }

        @Override
        public String typeExpression() {
            return typeName;
        }

        @Override
        public OptionalInt length() {
            return length;
        }

        @Override
        public OptionalInt scale() {
            return OptionalInt.empty();
        }

        @Override
        public boolean isOptional() {
            return true;
        }

        @Override
        public Object defaultValue() {
            return null;
        }

        @Override
        public boolean hasDefaultValue() {
            return false;
        }

        @Override
        public String name() {
            return "c";
        }

        @Override
        public String dataCollection() {
            return "db.t";
        }
    }
}
