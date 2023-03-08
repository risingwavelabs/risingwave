package com.risingwave.connector;

import static io.grpc.Status.*;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.proto.Data;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.types.*;
import io.delta.standalone.util.ParquetSchemaConverter;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;

class DeltaLakeSinkUtil {
    public static void checkSchema(TableSchema tableSchema, StructType schema) {
        if (schema == null) {
            throw INVALID_ARGUMENT
                    .withDescription("Schema of delta table is null")
                    .asRuntimeException();
        }
        Map<String, Data.DataType.TypeName> tableColumnTypes = tableSchema.getColumnTypes();
        Map<String, StructField> columnTypes =
                Arrays.stream(schema.getFields())
                        .collect(Collectors.toMap(StructField::getName, column -> column));
        // check that all columns in tableSchema exist in the delta lake table
        for (String tableColumnName : tableColumnTypes.keySet()) {
            if (!columnTypes.containsKey(tableColumnName)) {
                throw INVALID_ARGUMENT
                        .withDescription(
                                String.format(
                                        "Delta table should contain column %s", tableColumnName))
                        .asRuntimeException();
            }
        }
        // check that all required columns in the delta lake table exist in tableSchema
        // and that existing column types match
        for (Map.Entry<String, StructField> column : columnTypes.entrySet()) {
            Data.DataType.TypeName tableColumnType = tableColumnTypes.get(column.getKey());
            if (tableColumnType == null) {
                if (!column.getValue().isNullable()) {
                    throw INVALID_ARGUMENT
                            .withDescription(
                                    "Column in delta table which is not in sink should be nullable")
                            .asRuntimeException();
                }
            } else {
                DataType tableColumnDataType = convertType(tableColumnType);
                if (!tableColumnDataType.equals(column.getValue().getDataType())) {
                    throw INVALID_ARGUMENT
                            .withDescription(
                                    String.format(
                                            "Type of column %s is different", column.getKey()))
                            .asRuntimeException();
                }
            }
        }
    }

    private static DataType convertType(Data.DataType.TypeName typeName) {
        switch (typeName) {
            case INT16:
                return new ShortType();
            case INT32:
                return new IntegerType();
            case INT64:
                return new LongType();
            case FLOAT:
                return new FloatType();
            case DOUBLE:
                return new DoubleType();
            case BOOLEAN:
                return new BooleanType();
            case VARCHAR:
                return new StringType();
            case DECIMAL:
                return DecimalType.USER_DEFAULT;
            case TIMESTAMP:
                return new TimestampType();
            case DATE:
                return new DateType();
            case STRUCT:
            case LIST:
                throw UNIMPLEMENTED
                        .withDescription(String.format("not support %s now", typeName))
                        .asRuntimeException();
            case INTERVAL:
            case TIME:
            case TIMESTAMPTZ:
                throw INVALID_ARGUMENT
                        .withDescription(String.format("Illegal type %s in Delta Lake", typeName))
                        .asRuntimeException();
            default:
                throw INVALID_ARGUMENT
                        .withDescription("unspecified type" + typeName)
                        .asRuntimeException();
        }
    }

    public static Schema convertSchema(DeltaLog log, TableSchema tableSchema) {
        StructType schema = log.snapshot().getMetadata().getSchema();
        MessageType parquetSchema = ParquetSchemaConverter.deltaToParquet(schema);
        return new AvroSchemaConverter().convert(parquetSchema);
    }
}
