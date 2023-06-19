// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.risingwave.functions;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.*;
import org.apache.arrow.vector.types.pojo.*;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

class TypeUtils {
    /**
     * Convert a string to an Arrow type.
     */
    static Field stringToField(String typeStr, String name) {
        typeStr = typeStr.toUpperCase();
        if (typeStr.equals("BOOLEAN") || typeStr.equals("BOOL")) {
            return Field.nullable(name, new ArrowType.Bool());
        } else if (typeStr.equals("SMALLINT") || typeStr.equals("INT2")) {
            return Field.nullable(name, new ArrowType.Int(16, true));
        } else if (typeStr.equals("INT") || typeStr.equals("INTEGER") || typeStr.equals("INT4")) {
            return Field.nullable(name, new ArrowType.Int(32, true));
        } else if (typeStr.equals("BIGINT") || typeStr.equals("INT8")) {
            return Field.nullable(name, new ArrowType.Int(64, true));
        } else if (typeStr.equals("FLOAT4") || typeStr.equals("REAL")) {
            return Field.nullable(name, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
        } else if (typeStr.equals("FLOAT8") || typeStr.equals("DOUBLE PRECISION")) {
            return Field.nullable(name, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
        } else if (typeStr.startsWith("DECIMAL") || typeStr.startsWith("NUMERIC")) {
            return Field.nullable(name, new ArrowType.Decimal(38, 0, 128));
        } else if (typeStr.equals("DATE")) {
            return Field.nullable(name, new ArrowType.Date(DateUnit.DAY));
        } else if (typeStr.equals("TIME") || typeStr.equals("TIME WITHOUT TIME ZONE")) {
            return Field.nullable(name, new ArrowType.Time(TimeUnit.MICROSECOND, 64));
        } else if (typeStr.equals("TIMESTAMP") || typeStr.equals("TIMESTAMP WITHOUT TIME ZONE")) {
            return Field.nullable(name, new ArrowType.Timestamp(TimeUnit.MICROSECOND, null));
        } else if (typeStr.startsWith("INTERVAL")) {
            return Field.nullable(name, new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO));
        } else if (typeStr.equals("VARCHAR")) {
            return Field.nullable(name, new ArrowType.Utf8());
        } else if (typeStr.equals("JSONB")) {
            return Field.nullable(name, new ArrowType.LargeUtf8());
        } else if (typeStr.equals("BYTEA")) {
            return Field.nullable(name, new ArrowType.Binary());
        } else if (typeStr.endsWith("[]")) {
            Field innerField = stringToField(typeStr.substring(0, typeStr.length() - 2), "");
            return new Field(name, FieldType.nullable(new ArrowType.List()), Arrays.asList(innerField));
        } else if (typeStr.startsWith("STRUCT")) {
            // extract "STRUCT<INT, VARCHAR, ...>"
            var typeList = typeStr.substring(7, typeStr.length() - 1);
            var fields = Arrays.stream(typeList.split(","))
                    .map(s -> stringToField(s.trim(), ""))
                    .collect(Collectors.toList());
            return new Field(name, FieldType.nullable(new ArrowType.Struct()), fields);
        } else {
            throw new IllegalArgumentException("Unsupported type: " + typeStr);
        }
    }

    /**
     * Convert a Java class to an Arrow type.
     * 
     * @param param The Java class.
     * @param hint  An optional DataTypeHint annotation.
     * @param name  The name of the field.
     * @return The Arrow type.
     */
    static Field classToField(Class<?> param, DataTypeHint hint, String name) {
        if (hint != null) {
            return stringToField(hint.value(), name);
        } else if (param == Boolean.class || param == boolean.class) {
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
        } else if (param == BigDecimal.class) {
            return Field.nullable(name, new ArrowType.Decimal(38, 0, 128));
        } else if (param == LocalDate.class) {
            return Field.nullable(name, new ArrowType.Date(DateUnit.DAY));
        } else if (param == LocalTime.class) {
            return Field.nullable(name, new ArrowType.Time(TimeUnit.MICROSECOND, 64));
        } else if (param == LocalDateTime.class) {
            return Field.nullable(name, new ArrowType.Timestamp(TimeUnit.MICROSECOND, null));
        } else if (param == PeriodDuration.class) {
            return Field.nullable(name, new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO));
        } else if (param == String.class) {
            return Field.nullable(name, new ArrowType.Utf8());
        } else if (param == byte[].class) {
            return Field.nullable(name, new ArrowType.Binary());
        } else if (param.isArray()) {
            var innerField = classToField(param.getComponentType(), null, "");
            return new Field(name, FieldType.nullable(new ArrowType.List()), Arrays.asList(innerField));
        } else {
            // struct type
            var fields = new ArrayList<Field>();
            for (var field : param.getDeclaredFields()) {
                var subhint = field.getAnnotation(DataTypeHint.class);
                fields.add(classToField(field.getType(), subhint, field.getName()));
            }
            return new Field("", FieldType.nullable(new ArrowType.Struct()), fields);
            // TODO: more types
            // throw new IllegalArgumentException("Unsupported type: " + param);
        }
    }

    /**
     * Get the input schema from a Java method.
     */
    static Schema methodToInputSchema(Method method) {
        var fields = new ArrayList<Field>();
        for (var param : method.getParameters()) {
            var hint = param.getAnnotation(DataTypeHint.class);
            fields.add(classToField(param.getType(), hint, param.getName()));
        }
        return new Schema(fields);
    }

    /**
     * Get the output schema of a scalar function from a Java method.
     */
    static Schema methodToOutputSchema(Method method) {
        var type = method.getReturnType();
        var hint = method.getAnnotation(DataTypeHint.class);
        return new Schema(Arrays.asList(classToField(type, hint, "")));
    }

    /**
     * Get the output schema of a table function from a Java class.
     */
    static Schema tableFunctionToOutputSchema(Method method) {
        var hint = method.getAnnotation(DataTypeHint.class);
        var type = method.getReturnType();
        if (!Iterator.class.isAssignableFrom(type)) {
            throw new IllegalArgumentException("Table function must return Iterator");
        }
        var typeArguments = ((ParameterizedType) method.getGenericReturnType()).getActualTypeArguments();
        type = (Class<?>) typeArguments[0];
        var row_index = Field.nullable("row_index", new ArrowType.Int(32, true));
        return new Schema(Arrays.asList(row_index, classToField(type, hint, "")));
    }

    /**
     * Return functions to process input values from a Java method.
     */
    static Function<Object, Object>[] methodToProcessInputs(Method method) {
        var schema = methodToInputSchema(method);
        var params = method.getParameters();
        @SuppressWarnings("unchecked")
        Function<Object, Object>[] funcs = new Function[schema.getFields().size()];
        for (int i = 0; i < schema.getFields().size(); i++) {
            funcs[i] = processFunc(schema.getFields().get(i), params[i].getType());
        }
        return funcs;
    }

    /**
     * Create an Arrow vector from an array of values.
     */
    static FieldVector createVector(Field field, BufferAllocator allocator, Object[] values) {
        var vector = field.createVector(allocator);
        fillVector(vector, values);
        return vector;
    }

    /**
     * Fill an Arrow vector with an array of values.
     */
    static void fillVector(FieldVector fieldVector, Object[] values) {
        if (fieldVector instanceof BitVector) {
            var vector = (BitVector) fieldVector;
            vector.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                if (values[i] != null) {
                    vector.set(i, (boolean) values[i] ? 1 : 0);
                }
            }
        } else if (fieldVector instanceof SmallIntVector) {
            var vector = (SmallIntVector) fieldVector;
            vector.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                if (values[i] != null) {
                    vector.set(i, (short) values[i]);
                }
            }
        } else if (fieldVector instanceof IntVector) {
            var vector = (IntVector) fieldVector;
            vector.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                if (values[i] != null) {
                    vector.set(i, (int) values[i]);
                }
            }
        } else if (fieldVector instanceof BigIntVector) {
            var vector = (BigIntVector) fieldVector;
            vector.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                if (values[i] != null) {
                    vector.set(i, (long) values[i]);
                }
            }
        } else if (fieldVector instanceof Float4Vector) {
            var vector = (Float4Vector) fieldVector;
            vector.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                if (values[i] != null) {
                    vector.set(i, (float) values[i]);
                }
            }
        } else if (fieldVector instanceof Float8Vector) {
            var vector = (Float8Vector) fieldVector;
            vector.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                if (values[i] != null) {
                    vector.set(i, (double) values[i]);
                }
            }
        } else if (fieldVector instanceof DecimalVector) {
            var vector = (DecimalVector) fieldVector;
            vector.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                if (values[i] != null) {
                    vector.set(i, (BigDecimal) values[i]);
                }
            }
        } else if (fieldVector instanceof DateDayVector) {
            var vector = (DateDayVector) fieldVector;
            vector.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                if (values[i] != null) {
                    vector.set(i, (int) ((LocalDate) values[i]).toEpochDay());
                }
            }
        } else if (fieldVector instanceof TimeMicroVector) {
            var vector = (TimeMicroVector) fieldVector;
            vector.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                if (values[i] != null) {
                    vector.set(i, ((LocalTime) values[i]).toNanoOfDay() / 1000);
                }
            }
        } else if (fieldVector instanceof TimeStampMicroVector) {
            var vector = (TimeStampMicroVector) fieldVector;
            vector.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                if (values[i] != null) {
                    var datetime = (LocalDateTime) values[i];
                    var date = datetime.toLocalDate().toEpochDay();
                    var time = datetime.toLocalTime().toNanoOfDay();
                    vector.set(i, date * 24 * 3600 * 1000 * 1000 + time / 1000);
                }
            }
        } else if (fieldVector instanceof IntervalMonthDayNanoVector) {
            var vector = (IntervalMonthDayNanoVector) fieldVector;
            vector.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                if (values[i] != null) {
                    var pd = (PeriodDuration) values[i];
                    var months = (int) pd.getPeriod().toTotalMonths();
                    var days = pd.getPeriod().getDays();
                    var nanos = pd.getDuration().toNanos();
                    vector.set(i, months, days, nanos);
                }
            }
        } else if (fieldVector instanceof VarCharVector) {
            var vector = (VarCharVector) fieldVector;
            vector.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                if (values[i] != null) {
                    vector.set(i, ((String) values[i]).getBytes());
                }
            }
        } else if (fieldVector instanceof LargeVarCharVector) {
            var vector = (LargeVarCharVector) fieldVector;
            vector.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                if (values[i] != null) {
                    vector.set(i, ((String) values[i]).getBytes());
                }
            }
        } else if (fieldVector instanceof VarBinaryVector) {
            var vector = (VarBinaryVector) fieldVector;
            vector.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                if (values[i] != null) {
                    vector.set(i, (byte[]) values[i]);
                }
            }
        } else if (fieldVector instanceof ListVector) {
            var vector = (ListVector) fieldVector;
            vector.allocateNew();
            // we have to enumerate the inner type again
            if (vector.getDataVector() instanceof LargeVarCharVector) {
                var innerVector = (LargeVarCharVector) vector.getDataVector();
                for (int i = 0; i < values.length; i++) {
                    var array = (String[]) values[i];
                    if (array == null) {
                        continue;
                    }
                    vector.startNewValue(i);
                    for (int j = 0; j < array.length; j++) {
                        if (array[j] != null) {
                            innerVector.setSafe(j, array[j].getBytes());
                        }
                    }
                    vector.endValue(i, array.length);
                }
            } else {
                throw new IllegalArgumentException("Unsupported type: " + fieldVector.getClass());
            }
        } else if (fieldVector instanceof StructVector) {
            var vector = (StructVector) fieldVector;
            vector.allocateNew();
            var lookup = MethodHandles.lookup();
            for (var field : vector.getField().getChildren()) {
                // extract field from values
                var subvalues = new Object[values.length];
                if (values.length != 0) {
                    try {
                        var javaField = values[0].getClass().getDeclaredField(field.getName());
                        var varHandle = lookup.unreflectVarHandle(javaField);
                        for (int i = 0; i < values.length; i++) {
                            subvalues[i] = varHandle.get(values[i]);
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

    /**
     * Return a function that converts the object get from input array to the
     * correct type.
     */
    static Function<Object, Object> processFunc(Field field, Class<?> targetClass) {
        if (field.getType() instanceof ArrowType.Utf8 && targetClass == String.class) {
            // object is org.apache.arrow.vector.util.Text
            return obj -> obj == null ? null : obj.toString();
        } else if (field.getType() instanceof ArrowType.LargeUtf8 && targetClass == String.class) {
            // object is org.apache.arrow.vector.util.Text
            return obj -> obj == null ? null : obj.toString();
        } else if (field.getType() instanceof ArrowType.Date && targetClass == LocalDate.class) {
            // object is Integer
            return obj -> obj == null ? null : LocalDate.ofEpochDay((int) obj);
        } else if (field.getType() instanceof ArrowType.Time && targetClass == LocalTime.class) {
            // object is Long
            return obj -> obj == null ? null : LocalTime.ofNanoOfDay((long) obj * 1000);
        } else if (field.getType() instanceof ArrowType.Interval && targetClass == PeriodDuration.class) {
            // object is arrow PeriodDuration
            return obj -> obj == null ? null : new PeriodDuration((org.apache.arrow.vector.PeriodDuration) obj);
        } else if (field.getType() instanceof ArrowType.List) {
            // object is org.apache.arrow.vector.util.JsonStringArrayList
            var subfield = field.getChildren().get(0);
            var subfunc = processFunc(subfield, targetClass.getComponentType());
            if (subfield.getType() instanceof ArrowType.Utf8) {
                return obj -> obj == null ? null : ((List<?>) obj).stream().map(subfunc).toArray(String[]::new);
            } else if (subfield.getType() instanceof ArrowType.LargeUtf8) {
                return obj -> obj == null ? null : ((List<?>) obj).stream().map(subfunc).toArray(String[]::new);
            }
            throw new IllegalArgumentException("Unsupported type: " + field.getType());
        } else if (field.getType() instanceof ArrowType.Struct) {
            // object is org.apache.arrow.vector.util.JsonStringHashMap
            var subfields = field.getChildren();
            @SuppressWarnings("unchecked")
            Function<Object, Object>[] subfunc = new Function[subfields.size()];
            for (int i = 0; i < subfields.size(); i++) {
                subfunc[i] = processFunc(subfields.get(i), targetClass.getFields()[i].getType());
            }
            return obj -> {
                if (obj == null)
                    return null;
                var map = (AbstractMap<?, ?>) obj;
                try {
                    var row = targetClass.getDeclaredConstructor().newInstance();
                    for (int i = 0; i < subfields.size(); i++) {
                        var field0 = targetClass.getFields()[i];
                        var val = subfunc[i].apply(map.get(field0.getName()));
                        field0.set(row, val);
                    }
                    return row;
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException
                        | NoSuchMethodException e) {
                    throw new RuntimeException(e);
                }
            };
        }
        return Function.identity();
    }
}
