// Copyright 2025 RisingWave Labs
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
import com.risingwave.connector.api.sink.CloseableIterable;
import com.risingwave.connector.api.sink.Deserializer;
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.java.binding.StreamChunk;
import com.risingwave.java.binding.StreamChunkIterator;
import com.risingwave.java.binding.StreamChunkRow;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.ConnectorServiceProto.SinkWriterStreamRequest.WriteBatch.StreamChunkPayload;
import com.risingwave.proto.Data;
import java.util.Iterator;

public class StreamChunkDeserializer implements Deserializer {
    interface ValueGetter {
        Object get(StreamChunkRow row);
    }

    private final ValueGetter[] valueGetters;

    public StreamChunkDeserializer(TableSchema tableSchema) {
        this.valueGetters = buildValueGetter(tableSchema);
    }

    static ValueGetter[] buildValueGetter(TableSchema tableSchema) {
        String[] colNames = tableSchema.getColumnNames();
        ValueGetter[] ret = new ValueGetter[colNames.length];
        for (int i = 0; i < colNames.length; i++) {
            int index = i;
            var columnDesc = tableSchema.getColumnDesc(index);
            switch (columnDesc.getDataType().getTypeName()) {
                case INT16:
                    ret[i] =
                            row -> {
                                if (row.isNull(index)) {
                                    return null;
                                }
                                return row.getShort(index);
                            };
                    break;
                case INT32:
                    ret[i] =
                            row -> {
                                if (row.isNull(index)) {
                                    return null;
                                }
                                return row.getInt(index);
                            };
                    break;
                case INT64:
                    ret[i] =
                            row -> {
                                if (row.isNull(index)) {
                                    return null;
                                }
                                return row.getLong(index);
                            };
                    break;
                case FLOAT:
                    ret[i] =
                            row -> {
                                if (row.isNull(index)) {
                                    return null;
                                }
                                return row.getFloat(index);
                            };
                    break;
                case DOUBLE:
                    ret[i] =
                            row -> {
                                if (row.isNull(index)) {
                                    return null;
                                }
                                return row.getDouble(index);
                            };
                    break;
                case BOOLEAN:
                    ret[i] =
                            row -> {
                                if (row.isNull(index)) {
                                    return null;
                                }
                                return row.getBoolean(index);
                            };
                    break;
                case VARCHAR:
                    ret[i] =
                            row -> {
                                if (row.isNull(index)) {
                                    return null;
                                }
                                return row.getString(index);
                            };
                    break;
                case TIMESTAMP:
                    ret[i] =
                            row -> {
                                if (row.isNull(index)) {
                                    return null;
                                }
                                return row.getTimestamp(index);
                            };
                    break;
                case TIMESTAMPTZ:
                    ret[i] =
                            row -> {
                                if (row.isNull(index)) {
                                    return null;
                                }
                                return row.getTimestamptz(index);
                            };
                    break;
                case TIME:
                    ret[i] =
                            row -> {
                                if (row.isNull(index)) {
                                    return null;
                                }
                                return row.getTime(index);
                            };
                    break;
                case DECIMAL:
                    ret[i] =
                            row -> {
                                if (row.isNull(index)) {
                                    return null;
                                }
                                return row.getDecimal(index);
                            };
                    break;
                case DATE:
                    ret[i] =
                            row -> {
                                if (row.isNull(index)) {
                                    return null;
                                }
                                return row.getDate(index);
                            };
                    break;

                case INTERVAL:
                    ret[i] =
                            row -> {
                                if (row.isNull(index)) {
                                    return null;
                                }
                                return row.getInterval(index);
                            };
                    break;

                case JSONB:
                    ret[i] =
                            row -> {
                                if (row.isNull(index)) {
                                    return null;
                                }
                                return row.getJsonb(index);
                            };
                    break;

                case BYTEA:
                    ret[i] =
                            row -> {
                                if (row.isNull(index)) {
                                    return null;
                                }
                                return row.getBytea(index);
                            };
                    break;
                case LIST:
                    var fieldType = columnDesc.getDataType().getFieldType(0);
                    switch (fieldType.getTypeName()) {
                        case INT16:
                        case INT32:
                        case INT64:
                        case FLOAT:
                        case DOUBLE:
                        case VARCHAR:
                            break;
                        default:
                            throw io.grpc.Status.INVALID_ARGUMENT
                                    .withDescription(
                                            "stream_chunk: unsupported array with field type "
                                                    + fieldType.getTypeName())
                                    .asRuntimeException();
                    }
                    ret[i] =
                            row -> {
                                if (row.isNull(index)) {
                                    return null;
                                }
                                Object[] objArray = null;
                                switch (fieldType.getTypeName()) {
                                    case INT16:
                                        objArray = row.getArray(index, Short.class);
                                        break;
                                    case INT32:
                                        objArray = row.getArray(index, Integer.class);
                                        break;
                                    case INT64:
                                        objArray = row.getArray(index, Long.class);
                                        break;
                                    case FLOAT:
                                        objArray = row.getArray(index, Float.class);
                                        break;
                                    case DOUBLE:
                                        objArray = row.getArray(index, Double.class);
                                        break;
                                    case VARCHAR:
                                        objArray = row.getArray(index, String.class);
                                        break;
                                    default:
                                        break;
                                }

                                return objArray;
                            };
                    break;
                default:
                    throw io.grpc.Status.INVALID_ARGUMENT
                            .withDescription(
                                    "stream_chunk: unsupported data type "
                                            + columnDesc.getDataType().getTypeName())
                            .asRuntimeException();
            }
        }
        return ret;
    }

    @Override
    public CloseableIterable<SinkRow> deserialize(
            ConnectorServiceProto.SinkWriterStreamRequest.WriteBatch writeBatch) {
        if (writeBatch.hasStreamChunkPayload()) {
            StreamChunkPayload streamChunkPayload = writeBatch.getStreamChunkPayload();
            return new StreamChunkIterable(
                    StreamChunk.fromPayload(streamChunkPayload.getBinaryData().toByteArray()),
                    valueGetters);
        } else if (writeBatch.hasStreamChunkRefPointer()) {
            return new StreamChunkIterable(
                    StreamChunk.fromRefPointer(writeBatch.getStreamChunkRefPointer()),
                    valueGetters);
        } else {
            throw INVALID_ARGUMENT
                    .withDescription(
                            "expected StreamChunkPayload, got " + writeBatch.getPayloadCase())
                    .asRuntimeException();
        }
    }

    static class StreamChunkRowWrapper implements SinkRow {

        private final StreamChunkRow inner;
        private final ValueGetter[] valueGetters;

        StreamChunkRowWrapper(StreamChunkRow inner, ValueGetter[] valueGetters) {
            this.inner = inner;
            this.valueGetters = valueGetters;
        }

        @Override
        public Object get(int index) {
            return valueGetters[index].get(inner);
        }

        @Override
        public Data.Op getOp() {
            return inner.getOp();
        }

        @Override
        public int size() {
            return valueGetters.length;
        }
    }

    static class StreamChunkIteratorWrapper implements Iterator<SinkRow>, AutoCloseable {
        private final StreamChunkIterator iter;
        private final ValueGetter[] valueGetters;
        private StreamChunkRowWrapper row;

        public StreamChunkIteratorWrapper(StreamChunk chunk, ValueGetter[] valueGetters) {
            this.iter = new StreamChunkIterator(chunk);
            this.valueGetters = valueGetters;
            this.row = null;
        }

        @Override
        public void close() {
            iter.close();
        }

        @Override
        public boolean hasNext() {
            if (this.row != null) {
                throw new RuntimeException(
                        "cannot call hasNext again when there is row not consumed by next");
            }
            StreamChunkRow row = iter.next();
            if (row == null) {
                return false;
            }
            this.row = new StreamChunkRowWrapper(row, valueGetters);
            return true;
        }

        @Override
        public SinkRow next() {
            // Move the sink row outside
            SinkRow ret = this.row;
            this.row = null;
            return ret;
        }
    }

    static class StreamChunkIterable implements CloseableIterable<SinkRow> {
        private final StreamChunk chunk;
        private final ValueGetter[] valueGetters;
        private StreamChunkIteratorWrapper iter;

        public StreamChunkIterable(StreamChunk chunk, ValueGetter[] valueGetters) {
            this.chunk = chunk;
            this.valueGetters = valueGetters;
            this.iter = null;
        }

        void clearIter() {
            if (this.iter != null) {
                this.iter.close();
            }
        }

        @Override
        public void close() throws Exception {
            clearIter();
            chunk.close();
        }

        @Override
        public Iterator<SinkRow> iterator() {
            clearIter();
            this.iter = new StreamChunkIteratorWrapper(chunk, valueGetters);
            return this.iter;
        }
    }
}
