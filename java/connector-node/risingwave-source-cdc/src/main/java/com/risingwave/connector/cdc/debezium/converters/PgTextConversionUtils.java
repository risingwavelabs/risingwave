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

import io.debezium.connector.postgresql.UnchangedToastedReplicationMessageColumn;
import io.debezium.spi.converter.RelationalColumn;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Shared value and type helpers for the PostgreSQL text-representation converters. */
final class PgTextConversionUtils {

    // Must match `DEBEZIUM_UNAVAILABLE_VALUE` on the Rust side
    // (src/common/src/types/mod.rs).
    private static final String UNAVAILABLE_VALUE_PLACEHOLDER = "__debezium_unavailable_value";

    private PgTextConversionUtils() {}

    /**
     * Renders a scalar value as text, mapping Debezium's unchanged-TOAST sentinel to the
     * placeholder the RisingWave parser recognizes.
     */
    static String convertScalar(Object value) {
        if (value == null) {
            return null;
        }

        if (UnchangedToastedReplicationMessageColumn.isUnchangedToastedValue(value)) {
            return UNAVAILABLE_VALUE_PLACEHOLDER;
        }

        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }

        if (value instanceof ByteBuffer buffer) {
            return StandardCharsets.UTF_8.decode(buffer.duplicate()).toString();
        }

        return value.toString();
    }

    static List<String> convertArray(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof List<?> list) {
            // Pre-decoded form (rare for unknown composite arrays, but handle it).
            var out = new ArrayList<String>(list.size());

            for (Object element : list) {
                out.add(convertScalar(element));
            }

            return out;
        }

        if (UnchangedToastedReplicationMessageColumn.isUnchangedToastedValue(value)) {
            return List.of(UNAVAILABLE_VALUE_PLACEHOLDER);
        }

        // Under include.unknown.datatypes=true, Debezium hands us the raw
        // PG textual array form `{"(a,b)","(c,d)"}` as bytes / string.
        // Split it into individual element strings.
        return parsePgTextArray(convertScalar(value));
    }

    static boolean isPostgresArrayOf(RelationalColumn column, String elementTypeName) {
        return isPostgresArrayTypeName(column.typeName(), elementTypeName)
                || isPostgresArrayTypeExpression(column.typeExpression(), elementTypeName);
    }

    private static boolean isPostgresArrayTypeName(String typeName, String elementTypeName) {
        if (typeName == null) {
            return false;
        }

        var normalized = unquoteAndUnqualify(typeName.toLowerCase());

        return normalized.equals("_" + elementTypeName)
                || normalized.equals(elementTypeName + "[]");
    }

    private static boolean isPostgresArrayTypeExpression(
            String typeExpression, String elementTypeName) {
        if (typeExpression == null) {
            return false;
        }

        var normalized = typeExpression.toLowerCase().replace("\"", "").trim();
        var unqualified = unquoteAndUnqualify(normalized);

        return unqualified.equals("_" + elementTypeName)
                || unqualified.equals(elementTypeName + "[]")
                || (unqualified.startsWith(elementTypeName + "(") && unqualified.endsWith("[]"));
    }

    private static String unquoteAndUnqualify(String typeName) {
        var unquoted = typeName.replace("\"", "").trim();
        var dot = unquoted.lastIndexOf('.');

        return dot >= 0 ? unquoted.substring(dot + 1) : unquoted;
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
}
