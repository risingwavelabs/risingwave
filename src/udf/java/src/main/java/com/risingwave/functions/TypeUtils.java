package com.risingwave.functions;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.*;
import org.apache.arrow.vector.types.pojo.*;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

class TypeUtils {
    public static Field stringToField(String typeStr) {
        typeStr = typeStr.toUpperCase();
        if (typeStr.equals("BOOLEAN") || typeStr.equals("BOOL")) {
            return new Field("", FieldType.nullable(new ArrowType.Bool()), null);
        } else if (typeStr.equals("SMALLINT") || typeStr.equals("INT2")) {
            return new Field("", FieldType.nullable(new ArrowType.Int(16, true)), null);
        } else if (typeStr.equals("INT") || typeStr.equals("INTEGER") || typeStr.equals("INT4")) {
            return new Field("", FieldType.nullable(new ArrowType.Int(32, true)), null);
        } else if (typeStr.equals("BIGINT") || typeStr.equals("INT8")) {
            return new Field("", FieldType.nullable(new ArrowType.Int(64, true)), null);
        } else if (typeStr.equals("FLOAT4") || typeStr.equals("REAL")) {
            return new Field("", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null);
        } else if (typeStr.equals("FLOAT8") || typeStr.equals("DOUBLE PRECISION")) {
            return new Field("", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
        } else if (typeStr.startsWith("DECIMAL") || typeStr.startsWith("NUMERIC")) {
            return new Field("", FieldType.nullable(new ArrowType.Decimal(38, 28, 128)), null);
        } else if (typeStr.equals("DATE")) {
            return new Field("", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null);
        } else if (typeStr.equals("TIME") || typeStr.equals("TIME WITHOUT TIME ZONE")) {
            return new Field("", FieldType.nullable(new ArrowType.Time(TimeUnit.MILLISECOND, 32)), null);
        } else if (typeStr.equals("TIMESTAMP") || typeStr.equals("TIMESTAMP WITHOUT TIME ZONE")) {
            return new Field("", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null);
        } else if (typeStr.startsWith("INTERVAL")) {
            return new Field("", FieldType.nullable(new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO)), null);
        } else if (typeStr.equals("VARCHAR")) {
            return new Field("", FieldType.nullable(new ArrowType.Utf8()), null);
        } else if (typeStr.equals("JSONB")) {
            return new Field("", FieldType.nullable(new ArrowType.LargeUtf8()), null);
        } else if (typeStr.equals("BYTEA")) {
            return new Field("", FieldType.nullable(new ArrowType.Binary()), null);
        } else if (typeStr.endsWith("[]")) {
            Field innerField = stringToField(typeStr.substring(0, typeStr.length() - 2));
            return new Field("", FieldType.nullable(new ArrowType.List()), Arrays.asList(innerField));
        } else if (typeStr.startsWith("STRUCT")) {
            // extract "STRUCT<INT, VARCHAR, ...>"
            var typeList = typeStr.substring(7, typeStr.length() - 1);
            var fields = Arrays.stream(typeList.split(","))
                    .map(s -> stringToField(s.trim()))
                    .collect(Collectors.toList());
            return new Field("", FieldType.nullable(new ArrowType.Struct()), fields);
        } else {
            throw new IllegalArgumentException("Unsupported type: " + typeStr);
        }
    }

    public static Field classToField(Class<?> param) {
        if (param == Boolean.class || param == boolean.class) {
            return new Field("", FieldType.nullable(new ArrowType.Bool()), null);
        } else if (param == Short.class || param == short.class) {
            return new Field("", FieldType.nullable(new ArrowType.Int(16, true)), null);
        } else if (param == Integer.class || param == int.class) {
            return new Field("", FieldType.nullable(new ArrowType.Int(32, true)), null);
        } else if (param == Long.class || param == long.class) {
            return new Field("", FieldType.nullable(new ArrowType.Int(64, true)), null);
        } else if (param == Float.class || param == float.class) {
            return new Field("", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null);
        } else if (param == Double.class || param == double.class) {
            return new Field("", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
        } else if (param == String.class) {
            return new Field("", FieldType.nullable(new ArrowType.Utf8()), null);
        } else if (param == byte[].class) {
            return new Field("", FieldType.nullable(new ArrowType.Binary()), null);
        } else {
            // TODO: more types
            throw new IllegalArgumentException("Unsupported type: " + param);
        }
    }

    public static Schema methodToInputSchema(Method method) {
        var fields = new ArrayList<Field>();
        for (var type : method.getParameterTypes()) {
            fields.add(classToField(type));
        }
        return new Schema(fields);
    }

    public static Schema methodToOutputSchema(Method method) {
        var type = method.getReturnType();
        var fields = Arrays.asList(classToField(type));
        return new Schema(fields);
    }

    public static FieldVector createVector(Field field, BufferAllocator allocator, Object[] values) {
        var fieldVector = field.createVector(allocator);
        if (fieldVector instanceof SmallIntVector) {
            var vector = (SmallIntVector) fieldVector;
            vector.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                vector.set(i, (short) values[i]);
            }
        } else if (fieldVector instanceof IntVector) {
            var vector = (IntVector) fieldVector;
            vector.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                vector.set(i, (int) values[i]);
            }
        } else if (fieldVector instanceof BigIntVector) {
            var vector = (BigIntVector) fieldVector;
            vector.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                vector.set(i, (long) values[i]);
            }
        } else {
            throw new IllegalArgumentException("Unsupported type: " + fieldVector.getClass());
        }
        fieldVector.setValueCount(values.length);
        return fieldVector;
    }
}
