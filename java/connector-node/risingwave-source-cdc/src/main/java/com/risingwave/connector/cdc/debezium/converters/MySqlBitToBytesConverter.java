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

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import java.util.BitSet;
import java.util.Properties;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * Converts MySQL {@code BIT(n)} values with {@code n > 1} into big-endian bytes.
 *
 * <p>Debezium's default handler maps {@code BIT(n > 1)} to {@code io.debezium.data.Bits}, whose
 * bytes are little-endian. The RisingWave CDC backfill reads the same column directly from MySQL,
 * which returns big-endian bytes (e.g. {@code b'1010'} in {@code BIT(10)} is {@code \x000a}). This
 * converter makes the streaming path produce the same bytes as the backfill path: big-endian,
 * zero-padded on the left to {@code ceil(n/8)} bytes.
 *
 * <p>{@code BIT(1)} is left to Debezium, which maps it to a boolean.
 *
 * <p>The conversion is opt-in via {@code big.endian} (the {@code debezium.bit.bytea.big.endian}
 * source option). Sources created before it existed already store the little-endian binlog bytes,
 * possibly in primary keys, so switching them over would make later updates and deletes miss the
 * stored rows.
 */
public class MySqlBitToBytesConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    static final String BIG_ENDIAN = "big.endian";

    private boolean bigEndian = false;

    @Override
    public void configure(Properties props) {
        bigEndian = Boolean.parseBoolean(props.getProperty(BIG_ENDIAN, "false").trim());
    }

    @Override
    public void converterFor(
            RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        if (!bigEndian) {
            return;
        }
        int numBits = column.length().orElse(1);
        if ("BIT".equalsIgnoreCase(column.typeName()) && numBits > 1) {
            int numBytes = (numBits + Byte.SIZE - 1) / Byte.SIZE;
            registration.register(
                    SchemaBuilder.bytes().optional(), value -> convertBits(value, numBytes));
        }
    }

    static byte[] convertBits(Object value, int numBytes) {
        if (value == null) {
            return null;
        }
        // Binlog events: bit `i` of the BitSet is the bit with weight 2^i.
        if (value instanceof BitSet bits) {
            byte[] bytes = new byte[numBytes];
            for (int i = bits.nextSetBit(0);
                    i >= 0 && i < numBytes * Byte.SIZE;
                    i = bits.nextSetBit(i + 1)) {
                bytes[numBytes - 1 - i / Byte.SIZE] |= (byte) (1 << (i % Byte.SIZE));
            }
            return bytes;
        }
        // JDBC result sets and column default values: already big-endian, but may be shorter
        // than `numBytes`.
        if (value instanceof byte[] bytes) {
            if (bytes.length >= numBytes) {
                return bytes;
            }
            byte[] padded = new byte[numBytes];
            System.arraycopy(bytes, 0, padded, numBytes - bytes.length, bytes.length);
            return padded;
        }
        throw new IllegalArgumentException(
                "unexpected value type for MySQL BIT column: " + value.getClass().getName());
    }
}
