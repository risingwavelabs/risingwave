/*
 * Copyright 2023 RisingWave Labs
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

package com.risingwave.connector;

import static io.grpc.Status.INVALID_ARGUMENT;
import static io.grpc.Status.UNIMPLEMENTED;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.proto.Data;
import io.grpc.Status;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class IcebergSinkUtil {
    private static Type convertType(Data.DataType.TypeName typeName) {
        switch (typeName) {
            case INT16:
            case INT32:
                return Types.IntegerType.get();
            case INT64:
                return Types.LongType.get();
            case FLOAT:
                return Types.FloatType.get();
            case DOUBLE:
                return Types.DoubleType.get();
            case BOOLEAN:
                return Types.BooleanType.get();
            case VARCHAR:
                return Types.StringType.get();
            case DECIMAL:
                return Types.DecimalType.of(10, 0);
            case TIMESTAMP:
                return Types.TimestampType.withoutZone();
            case TIMESTAMPTZ:
                return Types.TimestampType.withZone();
            case DATE:
                return Types.DateType.get();
            case TIME:
                return Types.TimeType.get();
            case STRUCT:
            case LIST:
                throw UNIMPLEMENTED
                        .withDescription(String.format("not support %s now", typeName))
                        .asRuntimeException();
            case INTERVAL:
                throw INVALID_ARGUMENT
                        .withDescription(String.format("Illegal type %s in Delta Lake", typeName))
                        .asRuntimeException();
            default:
                throw INVALID_ARGUMENT
                        .withDescription("unspecified type" + typeName)
                        .asRuntimeException();
        }
    }

    public static void checkSchema(TableSchema tableSchema, Schema icebergSchema) {
        if (icebergSchema == null) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("Schema of iceberg table is null")
                    .asRuntimeException();
        }
        Map<String, Data.DataType.TypeName> tableColumnTypes = tableSchema.getColumnTypes();
        List<Types.NestedField> icebergNestedFields = icebergSchema.columns();
        // Check that all columns in tableSchema exist in the iceberg table and that existing column
        // types match.
        for (Map.Entry<String, Data.DataType.TypeName> tableColumnEntry :
                tableColumnTypes.entrySet()) {
            Types.NestedField field = icebergSchema.findField(tableColumnEntry.getKey());
            if (field == null) {
                throw Status.FAILED_PRECONDITION
                        .withDescription(
                                String.format(
                                        "The name of table schema does not match. Column name: %s.",
                                        tableColumnEntry.getKey()))
                        .asRuntimeException();
            }
            if (!convertType(tableColumnEntry.getValue()).equals(field.type())) {
                throw Status.FAILED_PRECONDITION
                        .withDescription(
                                String.format(
                                        "The type of table schema does not match. Column name: %s.",
                                        tableColumnEntry.getKey()))
                        .asRuntimeException();
            }
        }
        // Check that all required columns in the iceberg table exist in tableSchema.
        for (Types.NestedField field : icebergNestedFields) {
            if (tableColumnTypes.get(field.name()) == null) {
                throw Status.FAILED_PRECONDITION
                        .withDescription(String.format("missing a required field %s", field.name()))
                        .asRuntimeException();
            }
        }
    }
}
