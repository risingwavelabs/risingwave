package com.risingwave.connector;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.ConnectorServiceProto.SinkStreamRequest.WriteBatch.StreamChunkPayload;
import com.risingwave.proto.Data;
import com.risingwave.java.binding;
import scala.None;

import java.util.Iterator;

import static io.grpc.Status.INVALID_ARGUMENT;

public class StreamChunkDeserializer {
    @Override
    public Iterator<SinkRow> deserialize(Object payload){
        if (!(payload instanceof ConnectorServiceProto.SinkStreamRequest.WriteBatch.StreamChunkPayload)) {
            throw INVALID_ARGUMENT
                    .withDescription("expected StreamChunkPayload, got " + payload.getClass().getName())
                    .asRuntimeException();
        }
        StreamChunkPayload streamChunkPayload = (StreamChunkPayload)  payload;
        // figure out how to call from_protobuf and then return
        return Binding.streamChunkFromProtobuf(streamChunkPayload);
    }

    private static Object validateStreamChunkDataTypes(Data.DataType.TypeName typeName, Object value){

    }

}
