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

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.risingwave.connector.api.ColumnDesc;
import com.risingwave.proto.Data.DataType;
import com.risingwave.proto.Data.DataType.TypeName;
import io.grpc.Status;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public class CassandraUtil {
    private static String getCorrespondingCassandraType(DataType dataType) {
        switch (dataType.getTypeName()) {
            case INT16:
                return "smallint";
            case INT32:
                return "int";
            case INT64:
                return "bigint";
            case FLOAT:
                return "float";
            case DOUBLE:
                return "double";
            case BOOLEAN:
                return "boolean";
            case VARCHAR:
                return "text";
            case DECIMAL:
                return "decimal";
            case TIMESTAMP:
                return "timestamp";
            case TIMESTAMPTZ:
                return "timestamp";
            case DATE:
                return "date";
            case TIME:
                return "time";
            case BYTEA:
                return "blob";
            case LIST:
            case STRUCT:
                throw Status.UNIMPLEMENTED
                        .withDescription(String.format("not support %s now", dataType))
                        .asRuntimeException();
            case INTERVAL:
                return "duration";
            default:
                throw Status.INVALID_ARGUMENT
                        .withDescription("unspecified type" + dataType)
                        .asRuntimeException();
        }
    }

    public static void checkSchema(
            List<ColumnDesc> columnDescs, Map<String, String> cassandraColumnDescMap) {
        if (columnDescs.size() != cassandraColumnDescMap.size()) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("Don't match in the number of columns in the table")
                    .asRuntimeException();
        }
        for (ColumnDesc columnDesc : columnDescs) {
            if (!cassandraColumnDescMap.containsKey(columnDesc.getName())) {
                throw Status.FAILED_PRECONDITION
                        .withDescription(
                                String.format(
                                        "Don't match in the name, rw is %s cassandra can't find it",
                                        columnDesc.getName()))
                        .asRuntimeException();
            }
            if (!cassandraColumnDescMap
                    .get(columnDesc.getName())
                    .equals(getCorrespondingCassandraType(columnDesc.getDataType()))) {
                throw Status.FAILED_PRECONDITION
                        .withDescription(
                                String.format(
                                        "Don't match in the type, name is %s, cassandra is %s, rw is %s",
                                        columnDesc.getName(),
                                        cassandraColumnDescMap.get(columnDesc.getName()),
                                        getCorrespondingCassandraType(columnDesc.getDataType())))
                        .asRuntimeException();
            }
        }
    }

    public static Object convertRow(Object value, TypeName typeName) {
        switch (typeName) {
            case INT16:
            case INT32:
            case INT64:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case VARCHAR:
            case DECIMAL:
                return value;
            case TIMESTAMP:
            case TIMESTAMPTZ:
                return ((Timestamp) value).toInstant();
            case DATE:
                return ((Date) value).toLocalDate();
            case TIME:
                return ((Time) value).toLocalTime();
            case INTERVAL:
                return CqlDuration.from((String) value);
            case BYTEA:
                return ByteBuffer.wrap((byte[]) value);
            case LIST:
            case STRUCT:
                throw Status.UNIMPLEMENTED
                        .withDescription(String.format("not support %s now", typeName))
                        .asRuntimeException();
            default:
                throw Status.INVALID_ARGUMENT
                        .withDescription("unspecified type" + typeName)
                        .asRuntimeException();
        }
    }
}
