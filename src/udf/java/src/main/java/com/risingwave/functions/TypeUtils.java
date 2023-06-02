package com.risingwave.functions;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.*;
import org.apache.arrow.vector.types.pojo.*;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

class TypeUtils {
    public static Field stringToField(String typeStr) {
        typeStr = typeStr.toUpperCase();
        if (typeStr.equals("BOOLEAN") || typeStr.equals("BOOL")) {
            return Field.nullable("", new ArrowType.Bool());
        } else if (typeStr.equals("SMALLINT") || typeStr.equals("INT2")) {
            return Field.nullable("", new ArrowType.Int(16, true));
        } else if (typeStr.equals("INT") || typeStr.equals("INTEGER") || typeStr.equals("INT4")) {
            return Field.nullable("", new ArrowType.Int(32, true));
        } else if (typeStr.equals("BIGINT") || typeStr.equals("INT8")) {
            return Field.nullable("", new ArrowType.Int(64, true));
        } else if (typeStr.equals("FLOAT4") || typeStr.equals("REAL")) {
            return Field.nullable("", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
        } else if (typeStr.equals("FLOAT8") || typeStr.equals("DOUBLE PRECISION")) {
            return Field.nullable("", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
        } else if (typeStr.startsWith("DECIMAL") || typeStr.startsWith("NUMERIC")) {
            return Field.nullable("", new ArrowType.Decimal(38, 28, 128));
        } else if (typeStr.equals("DATE")) {
            return Field.nullable("", new ArrowType.Date(DateUnit.DAY));
        } else if (typeStr.equals("TIME") || typeStr.equals("TIME WITHOUT TIME ZONE")) {
            return Field.nullable("", new ArrowType.Time(TimeUnit.MICROSECOND, 32));
        } else if (typeStr.equals("TIMESTAMP") || typeStr.equals("TIMESTAMP WITHOUT TIME ZONE")) {
            return Field.nullable("", new ArrowType.Timestamp(TimeUnit.MICROSECOND, null));
        } else if (typeStr.startsWith("INTERVAL")) {
            return Field.nullable("", new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO));
        } else if (typeStr.equals("VARCHAR")) {
            return Field.nullable("", new ArrowType.Utf8());
        } else if (typeStr.equals("JSONB")) {
            return Field.nullable("", new ArrowType.LargeUtf8());
        } else if (typeStr.equals("BYTEA")) {
            return Field.nullable("", new ArrowType.Binary());
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

    public static Field classToField(Class<?> param, String name) {
        if (param == Boolean.class || param == boolean.class) {
            return Field.nullable(name, new ArrowType.Bool());
        } else if (param == Short.class || param == short.class) {
            return Field.nullable(name, new ArrowType.Int(16, true));
        } else if (param == Integer.class || param == int.class) {
            return Field.nullable(name, new ArrowType.Int(32, true));
        } else if (param == Long.class || param == long.class) {
            return Field.nullable(name, new ArrowType.Int(64, true));
        } else if (param == Float.class || param == float.class) {
            return Field.nullable(name, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
        } else if (param == Double.class || param == double.class) {
            return Field.nullable(name, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
        } else if (param == String.class) {
            return Field.nullable(name, new ArrowType.Utf8());
        } else if (param == byte[].class) {
            return Field.nullable(name, new ArrowType.Binary());
        } else if (param.isArray()) {
            var innerField = classToField(param.getComponentType(), "");
            return new Field(name, FieldType.nullable(new ArrowType.List()), Arrays.asList(innerField));
        } else {
            // struct type
            var fields = new ArrayList<Field>();
            for (var field : param.getDeclaredFields()) {
                fields.add(classToField(field.getType(), field.getName()));
            }
            return new Field("", FieldType.nullable(new ArrowType.Struct()), fields);
            // TODO: more types
            // throw new IllegalArgumentException("Unsupported type: " + param);
        }
    }

    public static Schema methodToInputSchema(Method method) {
        var fields = new ArrayList<Field>();
        for (var param : method.getParameters()) {
            fields.add(classToField(param.getType(), param.getName()));
        }
        return new Schema(fields);
    }

    public static Schema methodToOutputSchema(Method method) {
        var type = method.getReturnType();
        return new Schema(Arrays.asList(classToField(type, "")));
    }

    public static Schema tableFunctionToOutputSchema(Class<?> type) {
        var parameterizedType = (ParameterizedType) type.getGenericSuperclass();
        var typeArguments = parameterizedType.getActualTypeArguments();
        type = (Class<?>) typeArguments[0];

        var row_index = Field.nullable("row_index", new ArrowType.Int(32, true));
        return new Schema(Arrays.asList(row_index, classToField(type, "")));
    }

    public static FieldVector createVector(Field field, BufferAllocator allocator, Object[] values) {
        var vector = field.createVector(allocator);
        fillVector(vector, values);
        return vector;
    }

    static void fillVector(FieldVector fieldVector, Object[] values) {
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
        } else if (fieldVector instanceof Float4Vector) {
            var vector = (Float4Vector) fieldVector;
            vector.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                vector.set(i, (float) values[i]);
            }
        } else if (fieldVector instanceof Float8Vector) {
            var vector = (Float8Vector) fieldVector;
            vector.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                vector.set(i, (double) values[i]);
            }
        } else if (fieldVector instanceof DateDayVector) {
            var vector = (DateDayVector) fieldVector;
            vector.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                vector.set(i, (int) values[i]);
            }
        } else if (fieldVector instanceof TimeMicroVector) {
            var vector = (TimeMicroVector) fieldVector;
            vector.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                vector.set(i, (long) values[i]);
            }
        } else if (fieldVector instanceof TimeStampMicroVector) {
            var vector = (TimeStampMicroVector) fieldVector;
            vector.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                vector.set(i, (long) values[i]);
            }
        } else if (fieldVector instanceof VarCharVector) {
            var vector = (VarCharVector) fieldVector;
            vector.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                vector.set(i, ((String) values[i]).getBytes());
            }
        } else if (fieldVector instanceof VarBinaryVector) {
            var vector = (VarBinaryVector) fieldVector;
            vector.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                vector.set(i, (byte[]) values[i]);
            }
        } else if (fieldVector instanceof StructVector) {
            var vector = (StructVector) fieldVector;
            vector.allocateNew();
            for (var field : vector.getField().getChildren()) {
                // extract field from values
                var subvalues = new Object[values.length];
                if (values.length != 0) {
                    try {
                        var javaField = values[0].getClass().getDeclaredField(field.getName());
                        for (int i = 0; i < values.length; i++) {
                            subvalues[i] = javaField.get(values[i]);
                        }
                    } catch (NoSuchFieldException | IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }
                var subvector = vector.getChild(field.getName());
                fillVector(subvector, subvalues);
            }
            for (int i = 0; i < values.length; i++) {
                vector.setIndexDefined(i);
            }
        } else {
            throw new IllegalArgumentException("Unsupported type: " + fieldVector.getClass());
        }
        fieldVector.setValueCount(values.length);
    }
}
