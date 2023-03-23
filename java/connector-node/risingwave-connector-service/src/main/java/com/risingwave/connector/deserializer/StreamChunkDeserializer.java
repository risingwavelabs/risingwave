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
import com.risingwave.connector.api.sink.CloseableIterator;
import com.risingwave.connector.api.sink.Deserializer;
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.java.binding.StreamChunkIterator;
import com.risingwave.java.binding.StreamChunkRow;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.ConnectorServiceProto.SinkStreamRequest.WriteBatch.StreamChunkPayload;
import com.risingwave.proto.Data;

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
            Data.DataType.TypeName typeName = tableSchema.getColumnType(colNames[i]);
            switch (typeName) {
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
                                return row.getDateTime(index);
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
                default:
                    throw io.grpc.Status.INVALID_ARGUMENT
                            .withDescription("unsupported type " + typeName)
                            .asRuntimeException();
            }
        }
        return ret;
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
                new StreamChunkIterator(streamChunkPayload.getBinaryData().toByteArray()),
                valueGetters);
    }

    static class StreamChunkRowWrapper implements SinkRow {

        private boolean isClosed;
        private final StreamChunkRow inner;
        private final ValueGetter[] valueGetters;

        StreamChunkRowWrapper(StreamChunkRow inner, ValueGetter[] valueGetters) {
            this.inner = inner;
            this.valueGetters = valueGetters;
            this.isClosed = false;
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

        @Override
        public void close() {
            if (!isClosed) {
                this.isClosed = true;
                inner.close();
            }
        }
    }

    static class StreamChunkIteratorWrapper implements CloseableIterator<SinkRow> {
        private final StreamChunkIterator iter;
        private final ValueGetter[] valueGetters;
        private StreamChunkRowWrapper row;

        public StreamChunkIteratorWrapper(StreamChunkIterator iter, ValueGetter[] valueGetters) {
            this.iter = iter;
            this.valueGetters = valueGetters;
            this.row = null;
        }

        @Override
        public void close() {
            iter.close();
            try {
                if (row != null) {
                    row.close();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
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
}
