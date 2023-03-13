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

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.ArraySinkrow;
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.java.binding.KeyedRow;
import com.risingwave.java.binding.StreamChunkIterator;
import com.risingwave.java.binding.StreamChunkRow;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.ConnectorServiceProto.SinkStreamRequest.WriteBatch.StreamChunkPayload;
import com.risingwave.proto.Data;
import com.risingwave.java.binding;
import scala.None;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static io.grpc.Status.INVALID_ARGUMENT;

public class StreamChunkDeserializer implements Deserializer {
    private final TableSchema tableSchema;

    public StreamChunkDeserializer(TableSchema tableSchema) {
        this.tableSchema = tableSchema;
    }

    @Override
    public Iterator<SinkRow> deserialize(Object payload){
        if (!(payload instanceof StreamChunkPayload)) {
            throw INVALID_ARGUMENT
                    .withDescription("expected StreamChunkPayload, got " + payload.getClass().getName())
                    .asRuntimeException();
        }
        StreamChunkPayload streamChunkPayload = (StreamChunkPayload)  payload;
        List<SinkRow> sinkRowList = new ArrayList<SinkRow>();
        try (StreamChunkIterator iter = new StreamChunkIterator(streamChunkPayload.toByteArray())){
            while(true){
                try(StreamChunkRow row = iter.next()){
                    if(row == null){
                        break;
                    }
                    Object[] values = new Object[tableSchema.getNumColumns()];
                    for(String columnName : tableSchema.getColumnNames()){
                        int columnIdx = tableSchema.getColumnIndex(columnName);
                        Data.DataType.TypeName typeName =
                                tableSchema.getColumnType(columnName);
                        values[tableSchema.getColumnIndex(columnName)] =
                                validateStreamChunkDataTypes(
                                        typeName, columnIdx, row);
                    }
                    sinkRowList.add(new ArraySinkrow(row.getOp(),values));
                }
            }
        }
        return sinkRowList.iterator();
    }

    private static Object validateStreamChunkDataTypes(Data.DataType.TypeName typeName, int columnIdx, StreamChunkRow row) {
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
}
