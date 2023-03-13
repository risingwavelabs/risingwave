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
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.java.binding.StreamChunkIterator;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.ConnectorServiceProto.SinkStreamRequest.WriteBatch.StreamChunkPayload;
import com.risingwave.proto.Data;
import com.risingwave.java.binding;
import scala.None;

import java.util.Iterator;

import static io.grpc.Status.INVALID_ARGUMENT;

public class StreamChunkDeserializer implements Deserializer {
    public StreamChunkDeserializer(){}

    @Override
    public Iterator<SinkRow> deserialize(Object payload){
        // how to call iterator and convert it into Iterator<SinkRow>
    }

    public StreamChunkIterator getStreamChunkIterator(Object payload){
        if (!(payload instanceof ConnectorServiceProto.SinkStreamRequest.WriteBatch.StreamChunkPayload)) {
            throw INVALID_ARGUMENT
                    .withDescription("expected StreamChunkPayload, got " + payload.getClass().getName())
                    .asRuntimeException();
        }
        StreamChunkPayload streamChunkPayload = (StreamChunkPayload)  payload;
        return new StreamChunkIterator(streamChunkPayload.getBinaryData().toByteArray());
    }
}
