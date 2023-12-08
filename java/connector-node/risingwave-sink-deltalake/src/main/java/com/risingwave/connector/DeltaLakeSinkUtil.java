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
                if (!checkType(tableColumnType, column.getValue().getDataType())) {
                    throw INVALID_ARGUMENT
                            .withDescription(
                                    String.format(
                                            "Type of column %s is different", column.getKey()))
                            .asRuntimeException();
                }
            }
        }
    }

    public static boolean checkType(Data.DataType.TypeName typeName, DataType dataType) {
        switch (typeName) {
            case INT16:
                return dataType.equals(new ShortType());
            case INT32:
                return dataType.equals(new IntegerType());
            case INT64:
                return dataType.equals(new LongType());
            case FLOAT:
                return dataType.equals(new FloatType());
            case DOUBLE:
                return dataType.equals(new DoubleType());
            case BOOLEAN:
                return dataType.equals(new BooleanType());
            case VARCHAR:
                return dataType.equals(new StringType());
            case DECIMAL:
                return dataType.getTypeName().contains("decimal");
            case TIMESTAMP:
                return dataType.equals(new TimestampType());
            case DATE:
                return dataType.equals(new DateType());
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
        MessageType parquetSchema =
                ParquetSchemaConverter.deltaToParquet(
                        schema, ParquetSchemaConverter.ParquetOutputTimestampType.TIMESTAMP_MILLIS);
        return new AvroSchemaConverter().convert(parquetSchema);
    }
}
