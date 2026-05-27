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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.connect.data.SchemaBuilder;

/** Converts PostgreSQL composite values (and arrays of composites) into plain strings. */
public class PgCompositeToStringConverter
        implements CustomConverter<SchemaBuilder, RelationalColumn> {

    // PG FirstNormalObjectId: OIDs >= this are user-defined types.
    // Built-in array types (e.g. _int4, _text) sit below this threshold and go
    // through Debezium's standard handlers untouched.
    private static final int PG_FIRST_USER_OID = 16384;

    private boolean debug;

    @Override
    public void configure(Properties props) {
        debug = Boolean.parseBoolean(props.getProperty("debug", "false"));
    }

    @Override
    public void converterFor(
            RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        if (debug) {
            System.err.printf(
                    "PgCompositeToStringConverter field=%s.%s jdbcType=%d nativeType=%d typeName=%s typeExpression=%s optional=%s%n",
                    column.dataCollection(),
                    column.name(),
                    column.jdbcType(),
                    column.nativeType(),
                    column.typeName(),
                    column.typeExpression(),
                    column.isOptional());
        }

        if (column.jdbcType() == Types.STRUCT) {
            // Always optional: PG DELETE before-image fills non-PK columns
            // with null under REPLICA IDENTITY DEFAULT, regardless of
            // upstream NOT NULL.
            registration.register(SchemaBuilder.string().optional(), this::convertScalar);
            return;
        }
        if (isUserDefinedArray(column)) {
            var elementSchema = SchemaBuilder.string().optional().build();
            registration.register(
                    SchemaBuilder.array(elementSchema).optional(), this::convertArray);
        }
    }

    private static boolean isUserDefinedArray(RelationalColumn column) {
        // ARRAY columns whose element OID is user-defined are treated as
        // arrays of composites. Built-in arrays (int[], text[], ...) have OIDs
        // below PG_FIRST_USER_OID and are skipped. User-defined enum/domain
        // arrays will also match and get rendered as text — acceptable
        // since the downstream column is varchar[] anyway.
        return column.jdbcType() == Types.ARRAY && column.nativeType() >= PG_FIRST_USER_OID;
    }

    private String convertScalar(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        if (value instanceof ByteBuffer buffer) {
            return bufferToString(buffer);
        }
        return value.toString();
    }

    private List<String> convertArray(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof List<?> list) {
            // Pre-decoded form (rare for unknown composite arrays, but handle it).
            var out = new ArrayList<String>(list.size());
            for (Object el : list) {
                out.add(elementToString(el));
            }
            return out;
        }
        // Under include.unknown.datatypes=true, Debezium hands us the raw
        // PG textual array form `{"(a,b)","(c,d)"}` as bytes / string.
        // Split it into individual element strings.
        return parsePgTextArray(elementToString(value));
    }

    /**
     * Minimal PG textual-array parser, sufficient for composite arrays. Recognizes the standard
     * form {@code "{elem,elem,...}"} where each element is either NULL, a bare token, or a
     * double-quoted string with {@code \\} and {@code \"} escapes.
     */
    private static List<String> parsePgTextArray(String s) {
        if (s == null || s.length() < 2 || s.charAt(0) != '{' || s.charAt(s.length() - 1) != '}') {
            return List.of();
        }
        var out = new ArrayList<String>();
        int i = 1;
        int end = s.length() - 1;
        while (i < end) {
            if (s.charAt(i) == '"') {
                i++;
                var sb = new StringBuilder();
                while (i < end && s.charAt(i) != '"') {
                    if (s.charAt(i) == '\\' && i + 1 < end) {
                        sb.append(s.charAt(i + 1));
                        i += 2;
                    } else {
                        sb.append(s.charAt(i));
                        i++;
                    }
                }
                i++; // consume closing "
                out.add(sb.toString());
            } else {
                int start = i;
                while (i < end && s.charAt(i) != ',') {
                    i++;
                }
                var raw = s.substring(start, i);
                out.add("NULL".equals(raw) ? null : raw);
            }
            if (i < end && s.charAt(i) == ',') {
                i++;
            }
        }
        return out;
    }

    private static String elementToString(Object el) {
        if (el == null) {
            return null;
        }
        if (el instanceof byte[] eb) {
            return new String(eb, StandardCharsets.UTF_8);
        }
        if (el instanceof ByteBuffer ebb) {
            return bufferToString(ebb);
        }
        return el.toString();
    }

    private static String bufferToString(ByteBuffer buffer) {
        var readOnly = buffer.asReadOnlyBuffer();
        var bytes = new byte[readOnly.remaining()];
        readOnly.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
