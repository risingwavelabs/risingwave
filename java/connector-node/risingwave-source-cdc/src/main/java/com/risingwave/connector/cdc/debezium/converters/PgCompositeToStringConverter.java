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
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.postgresql.geometric.PGpoint;
import org.postgresql.geometric.PGpolygon;

/**
 * Converts PostgreSQL composite values (and arrays of composites) into plain strings, and narrowly
 * handles the built-in polygon scalar/array case.
 */
public class PgCompositeToStringConverter
        implements CustomConverter<SchemaBuilder, RelationalColumn> {

    // PG FirstNormalObjectId: OIDs >= this are user-defined types.
    // Built-in array types (e.g. _int4, _text) sit below this threshold and go
    // through Debezium's standard handlers untouched.
    private static final int PG_FIRST_USER_OID = 16384;

    // Must match `DEBEZIUM_UNAVAILABLE_VALUE` on the Rust side
    // (src/common/src/types/mod.rs).
    private static final String UNAVAILABLE_VALUE_PLACEHOLDER = "__debezium_unavailable_value";

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
        if (isPostgresPolygonScalar(column)) {
            // Built-in geometric polygon arrives as JDBC Types.OTHER. Render
            // it as text (e.g. `(0,0),(1,1)`) matching the RW VARCHAR column.
            // This must stay narrow: geometry/geography and other geometric
            // types (point/box/circle/...) are left to Debezium's handlers.
            registration.register(SchemaBuilder.string().optional(), this::convertPolygonScalar);
            return;
        }
        if (isPostgresPolygonArray(column)) {
            // Built-in polygon[] reports an element OID below the user type
            // threshold, so it is not caught by isUserDefinedArray. Render it
            // as an array of text to match the RW `List(VARCHAR)` column.
            var elementSchema = SchemaBuilder.string().optional().build();
            registration.register(
                    SchemaBuilder.array(elementSchema).optional(), this::convertPolygonArray);
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
        //
        // Do not intercept extension arrays that Debezium already knows how to
        // encode with a native schema. For example, PostGIS geometry[] arrives
        // as an array of geometry structs and RisingWave decodes each element
        // as EWKB bytea. Re-registering it as array(string) would make the
        // downstream bytea parser treat hex EWKB text as base64.
        return column.jdbcType() == Types.ARRAY
                && column.nativeType() >= PG_FIRST_USER_OID
                && !isDebeziumNativeExtensionArray(column);
    }

    private static boolean isDebeziumNativeExtensionArray(RelationalColumn column) {
        return isPostgresArrayOf(column, "geometry") || isPostgresArrayOf(column, "geography");
    }

    private static boolean isPostgresPolygonScalar(RelationalColumn column) {
        // Scalar polygon arrives as JDBC Types.OTHER with a built-in OID below
        // PG_FIRST_USER_OID, so it is not a user-defined type. Match only the
        // `polygon` type name / expression; never geometry/geography or other
        // geometric types.
        return column.jdbcType() == Types.OTHER
                && (isPostgresTypeRef(column.typeName(), "polygon")
                        || isPostgresTypeRef(column.typeExpression(), "polygon"));
    }

    private static boolean isPostgresPolygonArray(RelationalColumn column) {
        // polygon[] is a built-in array (`_polygon` / `polygon[]`) and would
        // otherwise be skipped by isUserDefinedArray because its element OID is
        // below the user threshold. Require JDBC ARRAY first so a non-array
        // RelationalColumn with misleading type metadata is never registered
        // with an array schema.
        return column.jdbcType() == Types.ARRAY
                && (isPostgresArrayTypeName(column.typeName(), "polygon")
                        || isPostgresArrayTypeExpression(column.typeExpression(), "polygon"));
    }

    private static boolean isPostgresTypeRef(String typeRef, String typeName) {
        if (typeRef == null) {
            return false;
        }
        var normalized = unquoteAndUnqualify(typeRef.toLowerCase().trim());
        return normalized.equals(typeName);
    }

    private static boolean isPostgresArrayOf(RelationalColumn column, String elementTypeName) {
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
        if (isDebeziumUnavailableValueObject(value)) {
            return List.of(UNAVAILABLE_VALUE_PLACEHOLDER);
        }
        // Under include.unknown.datatypes=true, Debezium hands us the raw
        // PG textual array form `{"(a,b)","(c,d)"}` as bytes / string.
        // Split it into individual element strings.
        return parsePgTextArray(elementToString(value));
    }

    /**
     * Renders a polygon scalar as text in the form {@code ((x0,y0),...,(xn,yn))}, matching how
     * RisingWave's VARCHAR column stores the value. Null passes through unchanged; non-PGpolygon
     * values (byte / ByteBuffer / already-text) fall back to the generic scalar conversion.
     */
    private String convertPolygonScalar(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof PGpolygon polygon) {
            StringBuilder sb = new StringBuilder();
            sb.append('(');
            for (int i = 0; i < polygon.points.length; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                PGpoint p = polygon.points[i];
                sb.append('(')
                        .append(formatDouble(p.x))
                        .append(',')
                        .append(formatDouble(p.y))
                        .append(')');
            }
            sb.append(')');
            return sb.toString();
        }
        return convertScalar(value);
    }

    /**
     * Renders a polygon[] array as a {@code List<String>} of polygon scalars. A pre-decoded {@code
     * List} maps each element through polygon conversion while preserving nulls; the Debezium
     * unavailable sentinel keeps the current placeholder behavior; raw textual arrays are parsed
     * through the existing {@link #parsePgTextArray} parser without normalizing strings.
     */
    private List<String> convertPolygonArray(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof List<?> list) {
            var out = new ArrayList<String>(list.size());
            for (Object el : list) {
                out.add(convertPolygonScalar(el));
            }
            return out;
        }
        if (isDebeziumUnavailableValueObject(value)) {
            return List.of(UNAVAILABLE_VALUE_PLACEHOLDER);
        }
        return parsePgTextArray(elementToString(value));
    }

    /**
     * Formats a double the way PostgreSQL renders coordinates with {@code extra_float_digits > 0}
     * (shortest representation that round-trips). {@code NaN}, the infinities and {@code 0.0} /
     * {@code -0.0} are preserved explicitly. Finite non-zero values are rendered by progressively
     * reducing the {@link BigDecimal} precision (1..17 significant digits, {@link
     * RoundingMode#HALF_EVEN}) until {@code doubleValue()} bitwise round-trips back to the original
     * {@code double}, then emitting either plain notation (decimal exponent in [-4, 14]) or
     * PostgreSQL-style scientific notation for everything else. This mirrors upstream {@code
     * float8out} rather than {@link Double#toString} (which emits {@code 1.0E7} / {@code 1.0E-4} /
     * {@code 4.9E-324}).
     */
    static String formatDouble(double value) {
        if (Double.isNaN(value)) {
            return "NaN";
        }
        if (value == Double.POSITIVE_INFINITY) {
            return "Infinity";
        }
        if (value == Double.NEGATIVE_INFINITY) {
            return "-Infinity";
        }
        if (value == 0.0) {
            // +0.0 and -0.0 compare equal; detect the sign via the reciprocal.
            return 1.0 / value < 0 ? "-0" : "0";
        }
        BigDecimal original = BigDecimal.valueOf(value);
        for (int precision = 1; precision <= 17; precision++) {
            BigDecimal candidate =
                    original.round(new MathContext(precision, RoundingMode.HALF_EVEN));
            if (candidate.doubleValue() == value) {
                return formatBigDecimal(candidate.stripTrailingZeros());
            }
        }
        return formatBigDecimal(original.stripTrailingZeros());
    }

    /**
     * Renders a finite, non-zero {@link BigDecimal} with PostgreSQL {@code float8out} semantics.
     * Decimal exponent = precision - scale - 1; plain notation when it is in [-4, 14], otherwise
     * shortest scientific notation ({@code d.ddde±XX}, lowercase {@code e}, mandatory {@code +}
     * sign on non-negative exponents, and a two-digit minimum for the exponent magnitude).
     */
    private static String formatBigDecimal(BigDecimal value) {
        int exponent = value.precision() - value.scale() - 1;
        if (exponent >= -4 && exponent <= 14) {
            // e.g. 1e-4 -> "0.0001", 1e7 -> "10000000".
            return value.toPlainString();
        }
        // e.g. 1e-5 -> "1e-05", 1e20 -> "1e+20", 5e-324 -> "5e-324".
        String digits = value.unscaledValue().abs().toString();
        StringBuilder sb = new StringBuilder();
        if (value.signum() < 0) {
            sb.append('-');
        }
        sb.append(digits.charAt(0));
        if (digits.length() > 1) {
            sb.append('.').append(digits.substring(1));
        }
        sb.append('e');
        int absExponent = Math.abs(exponent);
        sb.append(exponent >= 0 ? '+' : '-')
                .append(absExponent < 10 ? "0" : "")
                .append(absExponent);
        return sb.toString();
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

    private static boolean isDebeziumUnavailableValueObject(Object value) {
        // Debezium's unchanged-TOAST sentinel for non-STRING schema columns is
        // a bare java.lang.Object singleton. Normal composite array values
        // arrive as concrete subclasses (byte[] / ByteBuffer / String / ...).
        return value != null && value.getClass() == Object.class;
    }

    private static String elementToString(Object el) {
        if (el == null) {
            return null;
        }
        if (isDebeziumUnavailableValueObject(el)) {
            return UNAVAILABLE_VALUE_PLACEHOLDER;
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
