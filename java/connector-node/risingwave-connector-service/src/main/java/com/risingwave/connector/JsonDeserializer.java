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

    private static Object validateJsonDataTypes(Data.DataType.TypeName typeName, Object value) {
        if (value instanceof Double
                && (Double) value % 1 == 0
                && typeName != Data.DataType.TypeName.DOUBLE
                && typeName != Data.DataType.TypeName.FLOAT) {
            return (int) (double) value;
        }
        switch (typeName) {
            case INT32:
            case INT64:
            case INT16:
                if (!(value instanceof Integer)) {
                    throw io.grpc.Status.INVALID_ARGUMENT
                            .withDescription("Expected int, got " + value.getClass())
                            .asRuntimeException();
                }
                break;
            case VARCHAR:
                if (!(value instanceof String)) {
                    throw io.grpc.Status.INVALID_ARGUMENT
                            .withDescription("Expected string, got " + value.getClass())
                            .asRuntimeException();
                }
                break;
            case DOUBLE:
                if (!(value instanceof Double)) {
                    throw io.grpc.Status.INVALID_ARGUMENT
                            .withDescription("Expected double, got " + value.getClass())
                            .asRuntimeException();
                }
                break;
            case FLOAT:
                if (!(value instanceof Float)) {
                    throw io.grpc.Status.INVALID_ARGUMENT
                            .withDescription("Expected float, got " + value.getClass())
                            .asRuntimeException();
                }
                break;
            case DECIMAL:
                if (!(value instanceof Float || value instanceof Double)) {
                    throw io.grpc.Status.INVALID_ARGUMENT
                            .withDescription("Expected float, got " + value.getClass())
                            .asRuntimeException();
                }
                break;
            case BOOLEAN:
                if (!(value instanceof Boolean)) {
                    throw io.grpc.Status.INVALID_ARGUMENT
                            .withDescription("Expected boolean, got " + value.getClass())
                            .asRuntimeException();
                }
                break;
            default:
                throw io.grpc.Status.INVALID_ARGUMENT
                        .withDescription("unsupported type " + typeName)
                        .asRuntimeException();
        }
        return value;
    }
}
