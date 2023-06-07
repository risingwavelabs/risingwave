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

package com.risingwave.connector;

import static io.grpc.Status.INVALID_ARGUMENT;

import com.google.gson.Gson;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.*;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.ConnectorServiceProto.SinkStreamRequest.WriteBatch.JsonPayload;
import com.risingwave.proto.Data;
import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Base64;
import java.util.Map;

public class JsonDeserializer implements Deserializer {
    private final TableSchema tableSchema;

    public JsonDeserializer(TableSchema tableSchema) {
        this.tableSchema = tableSchema;
    }

    // Encoding here should be consistent with `datum_to_json_object()` in
    // src/connector/src/sink/mod.rs
    @Override
    public CloseableIterator<SinkRow> deserialize(
            ConnectorServiceProto.SinkStreamRequest.WriteBatch writeBatch) {
        if (!writeBatch.hasJsonPayload()) {
            throw INVALID_ARGUMENT
                    .withDescription("expected JsonPayload, got " + writeBatch.getPayloadCase())
                    .asRuntimeException();
        }
        JsonPayload jsonPayload = writeBatch.getJsonPayload();
        return new TrivialCloseIterator<>(
                jsonPayload.getRowOpsList().stream()
                        .map(
                                rowOp -> {
                                    Map columnValues =
                                            new Gson().fromJson(rowOp.getLine(), Map.class);
                                    Object[] values = new Object[columnValues.size()];
                                    for (String columnName : tableSchema.getColumnNames()) {
                                        if (!columnValues.containsKey(columnName)) {
                                            throw INVALID_ARGUMENT
                                                    .withDescription(
                                                            "column "
                                                                    + columnName
                                                                    + " not found in json")
                                                    .asRuntimeException();
                                        }
                                        Data.DataType.TypeName typeName =
                                                tableSchema.getColumnType(columnName);
                                        values[tableSchema.getColumnIndex(columnName)] =
                                                validateJsonDataTypes(
                                                        typeName, columnValues.get(columnName));
                                    }
                                    return (SinkRow) new ArraySinkRow(rowOp.getOpType(), values);
                                })
                        .iterator());
    }

    private static Long castLong(Object value) {
        if (value instanceof Integer) {
            return ((Integer) value).longValue();
        } else if (value instanceof Double) {
            double d = (Double) value;
            if (d % 1.0 != 0.0) {

                throw io.grpc.Status.INVALID_ARGUMENT
                        .withDescription(
                                "unable to cast into long from non-integer double value: " + d)
                        .asRuntimeException();
            }
            return ((Double) value).longValue();
        } else if (value instanceof Long) {
            return (Long) value;
        } else if (value instanceof Short) {
            return ((Short) value).longValue();
        } else if (value instanceof Float) {
            double f = (Float) value;
            if (f % 1.0 != 0.0) {

                throw io.grpc.Status.INVALID_ARGUMENT
                        .withDescription(
                                "unable to cast into long from non-integer float value: " + f)
                        .asRuntimeException();
            }
            return ((Float) value).longValue();
        } else {
            throw io.grpc.Status.INVALID_ARGUMENT
                    .withDescription("unable to cast into long from " + value.getClass())
                    .asRuntimeException();
        }
    }

    private static Double castDouble(Object value) {
        if (value instanceof Double) {
            return (Double) value;
        } else if (value instanceof Float) {
            return ((Float) value).doubleValue();
        } else {
            throw io.grpc.Status.INVALID_ARGUMENT
                    .withDescription("unable to cast into double from " + value.getClass())
                    .asRuntimeException();
        }
    }

    private static BigDecimal castDecimal(Object value) {
        if (value instanceof String) {
            // FIXME(eric): See `datum_to_json_object()` in src/connector/src/sink/mod.rs
            return new BigDecimal((String) value);
        } else if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        } else {
            throw io.grpc.Status.INVALID_ARGUMENT
                    .withDescription("unable to cast into double from " + value.getClass())
                    .asRuntimeException();
        }
    }

    private static Time castTime(Object value) {
        try {
            Long milli = castLong(value);
            return new Time(milli);
        } catch (RuntimeException e) {
            throw io.grpc.Status.INVALID_ARGUMENT
                    .withDescription("unable to cast into time from " + value.getClass())
                    .asRuntimeException();
        }
    }

    private static Date castDate(Object value) {
        try {
            Long days = castLong(value) - 1;
            return Date.valueOf(LocalDate.of(1, 1, 1).plusDays(days));
        } catch (RuntimeException e) {
            throw io.grpc.Status.INVALID_ARGUMENT
                    .withDescription("unable to cast into date from " + value.getClass())
                    .asRuntimeException();
        }
    }

    private static Object validateJsonDataTypes(Data.DataType.TypeName typeName, Object value) {
        // value might be null
        if (value == null) {
            return null;
        }
        switch (typeName) {
            case INT16:
                return castLong(value).shortValue();
            case INT32:
                return castLong(value).intValue();
            case INT64:
                return castLong(value);
            case VARCHAR:
                if (!(value instanceof String)) {
                    throw io.grpc.Status.INVALID_ARGUMENT
                            .withDescription("Expected string, got " + value.getClass())
                            .asRuntimeException();
                }
                return value;
            case DOUBLE:
                return castDouble(value);
            case FLOAT:
                return castDouble(value).floatValue();
            case DECIMAL:
                return castDecimal(value);
            case BOOLEAN:
                if (!(value instanceof Boolean)) {
                    throw io.grpc.Status.INVALID_ARGUMENT
                            .withDescription("Expected boolean, got " + value.getClass())
                            .asRuntimeException();
                }
                return value;
            case TIMESTAMP:
            case TIMESTAMPTZ:
                if (!(value instanceof String)) {
                    throw io.grpc.Status.INVALID_ARGUMENT
                            .withDescription(
                                    "Expected timestamp in string, got " + value.getClass())
                            .asRuntimeException();
                }
                return Timestamp.valueOf((String) value);
            case TIME:
                return castTime(value);
            case DATE:
                return castDate(value);
            case INTERVAL:
                if (!(value instanceof String)) {
                    throw io.grpc.Status.INVALID_ARGUMENT
                            .withDescription("Expected interval, got " + value.getClass())
                            .asRuntimeException();
                }
                return value;
            case JSONB:
                if (!(value instanceof String)) {
                    throw io.grpc.Status.INVALID_ARGUMENT
                            .withDescription("Expected jsonb, got " + value.getClass())
                            .asRuntimeException();
                }
                return value;
            case BYTEA:
                if (!(value instanceof String)) {
                    throw io.grpc.Status.INVALID_ARGUMENT
                            .withDescription("Expected bytea, got " + value.getClass())
                            .asRuntimeException();
                }
                byte[] bytes = Base64.getDecoder().decode((String) value);
                return new ByteArrayInputStream(bytes);
            case LIST:
                if (!(value instanceof java.util.ArrayList<?>)) {
                    throw io.grpc.Status.INVALID_ARGUMENT
                            .withDescription("Expected list, got " + value.getClass())
                            .asRuntimeException();
                }
                return value;
            default:
                throw io.grpc.Status.INVALID_ARGUMENT
                        .withDescription("unsupported type " + typeName)
                        .asRuntimeException();
        }
    }
}
