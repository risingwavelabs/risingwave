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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import java.nio.ByteBuffer;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;
import org.postgresql.geometric.PGpoint;
import org.postgresql.geometric.PGpolygon;

public class PgCompositeToStringConverterTest {

    private static class TestColumn implements RelationalColumn {
        private final String name;
        private final String dataCollection;
        private final int jdbcType;
        private final int nativeType;
        private final String typeName;
        private final String typeExpression;
        private final boolean optional;
        private final Object defaultValue;
        private final boolean hasDefaultValue;

        TestColumn(
                String name,
                String dataCollection,
                int jdbcType,
                int nativeType,
                String typeName,
                String typeExpression,
                boolean optional,
                Object defaultValue,
                boolean hasDefaultValue) {
            this.name = name;
            this.dataCollection = dataCollection;
            this.jdbcType = jdbcType;
            this.nativeType = nativeType;
            this.typeName = typeName;
            this.typeExpression = typeExpression;
            this.optional = optional;
            this.defaultValue = defaultValue;
            this.hasDefaultValue = hasDefaultValue;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String dataCollection() {
            return dataCollection;
        }

        @Override
        public int jdbcType() {
            return jdbcType;
        }

        @Override
        public int nativeType() {
            return nativeType;
        }

        @Override
        public String typeName() {
            return typeName;
        }

        @Override
        public String typeExpression() {
            return typeExpression;
        }

        @Override
        public OptionalInt length() {
            return OptionalInt.empty();
        }

        @Override
        public OptionalInt scale() {
            return OptionalInt.empty();
        }

        @Override
        public boolean isOptional() {
            return optional;
        }

        @Override
        public Object defaultValue() {
            return defaultValue;
        }

        @Override
        public boolean hasDefaultValue() {
            return hasDefaultValue;
        }
    }

    private static class Capture implements CustomConverter.ConverterRegistration<SchemaBuilder> {
        SchemaBuilder schema;
        CustomConverter.Converter converter;

        @Override
        public void register(SchemaBuilder schema, CustomConverter.Converter converter) {
            this.schema = schema;
            this.converter = converter;
        }
    }

    private CustomConverter.Converter capturingConverter(TestColumn column) {
        PgCompositeToStringConverter subject = new PgCompositeToStringConverter();
        Capture capture = new Capture();
        subject.converterFor(column, capture);
        assertNotNull("converter should be registered", capture.converter);
        return capture.converter;
    }

    private CustomConverter.Converter capturedOrNull(TestColumn column) {
        PgCompositeToStringConverter subject = new PgCompositeToStringConverter();
        Capture capture = new Capture();
        subject.converterFor(column, capture);
        return capture.converter;
    }

    private TestColumn polygonScalarColumn() {
        return new TestColumn(
                "poly",
                "public.polygon",
                Types.OTHER,
                604,
                "public.\"polygon\"",
                "public.\"polygon\"",
                true,
                null,
                false);
    }

    private TestColumn polygonArrayColumn() {
        return new TestColumn(
                "poly_arr",
                "public.polygon[]",
                Types.ARRAY,
                1027,
                "public.\"_polygon\"",
                "public.\"polygon\"[]",
                true,
                null,
                false);
    }

    private TestColumn geometryColumn() {
        return new TestColumn(
                "geom",
                "public.geometry",
                Types.OTHER,
                20000,
                "geometry",
                "geometry",
                true,
                null,
                false);
    }

    private TestColumn geographyColumn() {
        return new TestColumn(
                "geog",
                "public.geography",
                Types.OTHER,
                20000,
                "geography",
                "geography",
                true,
                null,
                false);
    }

    private TestColumn pointColumn() {
        return new TestColumn(
                "pt", "public.point", Types.OTHER, 600, "point", "point", true, null, false);
    }

    private TestColumn geometryArrayColumn() {
        return new TestColumn(
                "geom_arr",
                "public.geometry[]",
                Types.ARRAY,
                20000,
                "_geometry",
                "geometry[]",
                true,
                null,
                false);
    }

    private TestColumn geographyArrayColumn() {
        return new TestColumn(
                "geog_arr",
                "public.geography[]",
                Types.ARRAY,
                20000,
                "_geography",
                "geography[]",
                true,
                null,
                false);
    }

    private TestColumn structColumn() {
        return new TestColumn(
                "comp",
                "public.composite",
                Types.STRUCT,
                0,
                "composite",
                "composite",
                true,
                null,
                false);
    }

    private TestColumn typeExpressionOnlyScalarColumn() {
        return new TestColumn(
                "poly",
                "public.polygon",
                Types.OTHER,
                604,
                null,
                "public.\"polygon\"",
                true,
                null,
                false);
    }

    private TestColumn typeExpressionOnlyArrayColumn() {
        return new TestColumn(
                "poly_arr",
                "public.polygon[]",
                Types.ARRAY,
                1027,
                null,
                "public.\"polygon\"[]",
                true,
                null,
                false);
    }

    @Test
    public void testScalarPolygonConversion() {
        CustomConverter.Converter converter = capturingConverter(polygonScalarColumn());
        PGpoint[] points = new PGpoint[] {new PGpoint(1, 1), new PGpoint(11, -2)};
        PGpolygon polygon = new PGpolygon(points);
        Object result = converter.convert(polygon);
        assertEquals("((1,1),(11,-2))", result);
    }

    @Test
    public void testScalarPolygonNonIntegral() {
        CustomConverter.Converter converter = capturingConverter(polygonScalarColumn());
        PGpoint[] points = new PGpoint[] {new PGpoint(-1.5, -2), new PGpoint(0.0, -0.0)};
        PGpolygon polygon = new PGpolygon(points);
        Object result = converter.convert(polygon);
        assertEquals("((-1.5,-2),(0,-0))", result);
    }

    @Test
    public void testScalarPolygonScientificRange() {
        // The formatter must produce PostgreSQL float8out text (extra_float_digits > 0)
        // rather than Double.toString for extreme magnitudes, and shortest scientific
        // notation out of the plain exponent window.
        CustomConverter.Converter converter = capturingConverter(polygonScalarColumn());
        PGpoint[] points =
                new PGpoint[] {
                    new PGpoint(1e7, 1e-4), // -> (10000000,0.0001)
                    new PGpoint(1e15, 1e-5), // -> (1e+15,1e-05)
                    new PGpoint(1e20, 1e-10), // -> (1e+20,1e-10)
                    new PGpoint(Double.MIN_VALUE, -Double.MIN_VALUE), // -> (5e-324,-5e-324)
                };
        PGpolygon polygon = new PGpolygon(points);
        Object result = converter.convert(polygon);
        assertEquals("((10000000,0.0001),(1e+15,1e-05),(1e+20,1e-10),(5e-324,-5e-324))", result);
    }

    @Test
    public void testScalarPolygonSpecialValues() {
        // NaN and the infinities are preserved verbatim; existing integer / decimal /
        // +/-0 handling must not regress.
        CustomConverter.Converter converter = capturingConverter(polygonScalarColumn());
        PGpoint[] points =
                new PGpoint[] {
                    new PGpoint(Double.NaN, Double.POSITIVE_INFINITY),
                    new PGpoint(Double.NEGATIVE_INFINITY, 0.25),
                    new PGpoint(-0.0, 0.0),
                };
        PGpolygon polygon = new PGpolygon(points);
        Object result = converter.convert(polygon);
        assertEquals("((NaN,Infinity),(-Infinity,0.25),(-0,0))", result);
    }

    @Test
    public void testScalarNull() {
        CustomConverter.Converter converter = capturingConverter(polygonScalarColumn());
        assertNull(converter.convert(null));
    }

    @Test
    public void testScalarStringFallback() {
        CustomConverter.Converter converter = capturingConverter(polygonScalarColumn());
        String input = "some string";
        assertSame(input, converter.convert(input));
    }

    @Test
    public void testArrayPolygonList() {
        CustomConverter.Converter converter = capturingConverter(polygonArrayColumn());
        PGpoint[] points1 = new PGpoint[] {new PGpoint(1, 1), new PGpoint(2, 2)};
        PGpoint[] points2 = new PGpoint[] {new PGpoint(3, 3), new PGpoint(4, 4)};
        List<Object> input = Arrays.asList(new PGpolygon(points1), null, new PGpolygon(points2));
        Object result = converter.convert(input);
        assertTrue(result instanceof List);
        List<?> list = (List<?>) result;
        assertEquals(3, list.size());
        assertEquals("((1,1),(2,2))", list.get(0));
        assertNull(list.get(1));
        assertEquals("((3,3),(4,4))", list.get(2));
    }

    @Test
    public void testArrayRawString() {
        CustomConverter.Converter converter = capturingConverter(polygonArrayColumn());
        String raw = "{\"((1.0,2.5),(3,4))\",NULL}";
        Object result = converter.convert(raw);
        assertTrue(result instanceof List);
        List<?> list = (List<?>) result;
        assertEquals(2, list.size());
        assertEquals("((1.0,2.5),(3,4))", list.get(0));
        assertNull(list.get(1));
    }

    @Test
    public void testScalarRegistrationTypeRefs() {
        PgCompositeToStringConverter subject = new PgCompositeToStringConverter();
        Capture capture = new Capture();
        subject.converterFor(polygonScalarColumn(), capture);
        assertNotNull(capture.converter);
        assertNotNull(capture.schema);
    }

    @Test
    public void testArrayRegistrationTypeRefs() {
        PgCompositeToStringConverter subject = new PgCompositeToStringConverter();
        Capture capture = new Capture();
        subject.converterFor(polygonArrayColumn(), capture);
        assertNotNull(capture.converter);
        assertNotNull(capture.schema);
    }

    @Test
    public void testScalarRegistrationByTypeExpressionOnly() {
        assertNotNull(capturingConverter(typeExpressionOnlyScalarColumn()));
    }

    @Test
    public void testArrayRegistrationByTypeExpressionOnly() {
        assertNotNull(capturingConverter(typeExpressionOnlyArrayColumn()));
    }

    @Test
    public void testOtherGeometryDoesNotRegister() {
        assertNull(capturedOrNull(geometryColumn()));
    }

    @Test
    public void testOtherGeographyDoesNotRegister() {
        assertNull(capturedOrNull(geographyColumn()));
    }

    @Test
    public void testOtherPointDoesNotRegister() {
        assertNull(capturedOrNull(pointColumn()));
    }

    @Test
    public void testArrayGeometryDoesNotRegister() {
        assertNull(capturedOrNull(geometryArrayColumn()));
    }

    @Test
    public void testArrayGeographyDoesNotRegister() {
        assertNull(capturedOrNull(geographyArrayColumn()));
    }

    @Test
    public void testStructCompositeRegisters() {
        CustomConverter.Converter converter = capturingConverter(structColumn());
        assertNotNull(converter);
    }

    @Test
    public void testStructString() {
        CustomConverter.Converter converter = capturingConverter(structColumn());
        String input = "hello";
        assertSame(input, converter.convert(input));
    }

    @Test
    public void testStructByteArray() {
        CustomConverter.Converter converter = capturingConverter(structColumn());
        byte[] input = "bytes".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Object result = converter.convert(input);
        assertEquals("bytes", result);
    }

    @Test
    public void testStructByteBuffer() {
        CustomConverter.Converter converter = capturingConverter(structColumn());
        ByteBuffer input =
                ByteBuffer.wrap("buffer".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        Object result = converter.convert(input);
        assertEquals("buffer", result);
    }

    @Test
    public void testStructNull() {
        CustomConverter.Converter converter = capturingConverter(structColumn());
        assertNull(converter.convert(null));
    }
}
