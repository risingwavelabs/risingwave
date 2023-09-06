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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
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
import java.util.Set;
import java.util.stream.Collectors;

public class CassandraUtil {
    private static int getCorrespondingCassandraType(DataType dataType) {
        switch (dataType.getTypeName()) {
            case INT16:
                return com.datastax.oss.protocol.internal.ProtocolConstants.DataType.SMALLINT;
            case INT32:
                return com.datastax.oss.protocol.internal.ProtocolConstants.DataType.INT;
            case INT64:
                return com.datastax.oss.protocol.internal.ProtocolConstants.DataType.BIGINT;
            case FLOAT:
                return com.datastax.oss.protocol.internal.ProtocolConstants.DataType.FLOAT;
            case DOUBLE:
                return com.datastax.oss.protocol.internal.ProtocolConstants.DataType.DOUBLE;
            case BOOLEAN:
                return com.datastax.oss.protocol.internal.ProtocolConstants.DataType.BOOLEAN;
            case VARCHAR:
                return com.datastax.oss.protocol.internal.ProtocolConstants.DataType.VARCHAR;
            case DECIMAL:
                return com.datastax.oss.protocol.internal.ProtocolConstants.DataType.DECIMAL;
            case TIMESTAMP:
                return com.datastax.oss.protocol.internal.ProtocolConstants.DataType.TIMESTAMP;
            case TIMESTAMPTZ:
                return com.datastax.oss.protocol.internal.ProtocolConstants.DataType.TIMESTAMP;
            case DATE:
                return com.datastax.oss.protocol.internal.ProtocolConstants.DataType.DATE;
            case TIME:
                return com.datastax.oss.protocol.internal.ProtocolConstants.DataType.TIME;
            case BYTEA:
                return com.datastax.oss.protocol.internal.ProtocolConstants.DataType.BLOB;
            case LIST:
            case STRUCT:
                throw Status.UNIMPLEMENTED
                        .withDescription(String.format("not support %s now", dataType))
                        .asRuntimeException();
            case INTERVAL:
                return com.datastax.oss.protocol.internal.ProtocolConstants.DataType.DURATION;
            default:
                throw Status.INVALID_ARGUMENT
                        .withDescription("unspecified type" + dataType)
                        .asRuntimeException();
        }
    }

    public static void checkSchema(
            List<ColumnDesc> columnDescs,
            Map<CqlIdentifier, ColumnMetadata> cassandraColumnDescMap) {
        if (columnDescs.size() != cassandraColumnDescMap.size()) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("Don't match in the number of columns in the table")
                    .asRuntimeException();
        }
        for (ColumnDesc columnDesc : columnDescs) {
            CqlIdentifier cql = CqlIdentifier.fromInternal(columnDesc.getName());
            if (!cassandraColumnDescMap.containsKey(cql)) {
                throw Status.FAILED_PRECONDITION
                        .withDescription(
                                String.format(
                                        "Don't match in the name, rw is %s cassandra can't find it",
                                        columnDesc.getName()))
                        .asRuntimeException();
            }
            if (cassandraColumnDescMap.get(cql).getType().getProtocolCode()
                    != getCorrespondingCassandraType(columnDesc.getDataType())) {
                throw Status.FAILED_PRECONDITION
                        .withDescription(
                                String.format(
                                        "Don't match in the type, name is %s, cassandra is %s, rw is %s",
                                        columnDesc.getName(),
                                        cassandraColumnDescMap.get(cql),
                                        columnDesc.getDataType().getTypeName()))
                        .asRuntimeException();
            }
        }
    }

    public static void checkPrimaryKey(
            List<ColumnMetadata> cassandraColumnMetadatas, List<String> columnMetadatas) {
        if (cassandraColumnMetadatas.size() != columnMetadatas.size()) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("Primary key len don't match")
                    .asRuntimeException();
        }
        Set<String> cassandraColumnsSet =
                cassandraColumnMetadatas.stream()
                        .map((a) -> a.getName().toString())
                        .collect(Collectors.toSet());
        for (String columnMetadata : columnMetadatas) {
            if (!cassandraColumnsSet.contains(columnMetadata)) {
                throw Status.FAILED_PRECONDITION
                        .withDescription(
                                String.format(
                                        "Primary key don't match. RisingWave Primary key is %s, don't find it in cassandra",
                                        columnMetadata))
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
