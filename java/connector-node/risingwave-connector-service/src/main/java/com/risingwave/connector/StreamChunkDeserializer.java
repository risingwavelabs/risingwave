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
