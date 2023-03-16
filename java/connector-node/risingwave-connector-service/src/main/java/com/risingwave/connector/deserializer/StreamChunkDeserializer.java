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

package com.risingwave.connector.deserializer;

import static io.grpc.Status.INVALID_ARGUMENT;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.ArraySinkRow;
import com.risingwave.connector.api.sink.CloseableIterator;
import com.risingwave.connector.api.sink.Deserializer;
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.java.binding.StreamChunkIterator;
import com.risingwave.java.binding.StreamChunkRow;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.ConnectorServiceProto.SinkStreamRequest.WriteBatch.StreamChunkPayload;
import com.risingwave.proto.Data;

public class StreamChunkDeserializer implements Deserializer {
    private final TableSchema tableSchema;

    public StreamChunkDeserializer(TableSchema tableSchema) {
        this.tableSchema = tableSchema;
    }

    @Override
    public CloseableIterator<SinkRow> deserialize(
            ConnectorServiceProto.SinkStreamRequest.WriteBatch writeBatch) {
        if (!writeBatch.hasStreamChunkPayload()) {
            throw INVALID_ARGUMENT
                    .withDescription(
                            "expected StreamChunkPayload, got " + writeBatch.getPayloadCase())
                    .asRuntimeException();
        }
        StreamChunkPayload streamChunkPayload = writeBatch.getStreamChunkPayload();
        return new StreamChunkIteratorWrapper(
                tableSchema,
                new StreamChunkIterator(streamChunkPayload.getBinaryData().toByteArray()));
    }

    private static Object validateStreamChunkDataTypes(
            Data.DataType.TypeName typeName, int columnIdx, StreamChunkRow row) {
        if (row.isNull(columnIdx)) {
            return null;
        }
        switch (typeName) {
            case INT16:
                return row.getShort(columnIdx);
            case INT32:
                return row.getInt(columnIdx);
            case INT64:
                return row.getLong(columnIdx);
            case FLOAT:
                return row.getFloat(columnIdx);
            case DOUBLE:
                return row.getDouble(columnIdx);
            case BOOLEAN:
                return row.getBoolean(columnIdx);
            case VARCHAR:
                return row.getString(columnIdx);
            default:
                throw io.grpc.Status.INVALID_ARGUMENT
                        .withDescription("unsupported type " + typeName)
                        .asRuntimeException();
        }
    }

    static class StreamChunkIteratorWrapper implements CloseableIterator<SinkRow> {
        private final TableSchema tableSchema;
        private final StreamChunkIterator iter;
        private StreamChunkRow row;

        public StreamChunkIteratorWrapper(TableSchema tableSchema, StreamChunkIterator iter) {
            this.tableSchema = tableSchema;
            this.iter = iter;
            this.row = null;
        }

        @Override
        public void close() {
            iter.close();
        }

        @Override
        public boolean hasNext() {
            row = iter.next();
            return row != null;
        }

        @Override
        public SinkRow next() {
            Object[] values = new Object[tableSchema.getNumColumns()];
            for (String columnName : tableSchema.getColumnNames()) {
                int columnIdx = tableSchema.getColumnIndex(columnName);
                Data.DataType.TypeName typeName = tableSchema.getColumnType(columnName);
                values[tableSchema.getColumnIndex(columnName)] =
                        validateStreamChunkDataTypes(typeName, columnIdx, row);
            }
            return new ArraySinkRow(row.getOp(), values);
        }
    }
}
