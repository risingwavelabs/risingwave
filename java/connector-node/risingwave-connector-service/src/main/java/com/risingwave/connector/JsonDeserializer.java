package com.risingwave.connector;

import static io.grpc.Status.INVALID_ARGUMENT;

import com.google.gson.Gson;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.ArraySinkrow;
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.proto.ConnectorServiceProto.SinkStreamRequest.WriteBatch.JsonPayload;
import com.risingwave.proto.Data;
import java.util.Iterator;
import java.util.Map;

public class JsonDeserializer implements Deserializer {
    private final TableSchema tableSchema;

    public JsonDeserializer(TableSchema tableSchema) {
        this.tableSchema = tableSchema;
    }

    @Override
    public Iterator<SinkRow> deserialize(Object payload) {
        if (!(payload instanceof JsonPayload)) {
            throw INVALID_ARGUMENT
                    .withDescription("expected JsonPayload, got " + payload.getClass().getName())
                    .asRuntimeException();
        }
        JsonPayload jsonPayload = (JsonPayload) payload;
        return jsonPayload.getRowOpsList().stream()
                .map(
                        rowOp -> {
                            Map columnValues = new Gson().fromJson(rowOp.getLine(), Map.class);
                            Object[] values = new Object[columnValues.size()];
                            for (String columnName : tableSchema.getColumnNames()) {
                                if (!columnValues.containsKey(columnName)) {
                                    throw INVALID_ARGUMENT
                                            .withDescription(
                                                    "column " + columnName + " not found in json")
                                            .asRuntimeException();
                                }
                                Data.DataType.TypeName typeName =
                                        tableSchema.getColumnType(columnName);
                                values[tableSchema.getColumnIndex(columnName)] =
                                        validateJsonDataTypes(
                                                typeName, columnValues.get(columnName));
                            }
                            return (SinkRow) new ArraySinkrow(rowOp.getOpType(), values);
                        })
                .iterator();
    }

    private static Long castLong(Object value) {
        if (value instanceof Integer) {
            return ((Integer) value).longValue();
        } else if (value instanceof Double) {
            return ((Double) value).longValue();
        } else if (value instanceof Long) {
            return (Long) value;
        } else if (value instanceof Short) {
            return ((Short) value).longValue();
        } else if (value instanceof Float) {
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

    private static Object validateJsonDataTypes(Data.DataType.TypeName typeName, Object value) {
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
            case BOOLEAN:
                if (!(value instanceof Boolean)) {
                    throw io.grpc.Status.INVALID_ARGUMENT
                            .withDescription("Expected boolean, got " + value.getClass())
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
