package com.risingwave.java.binding;

import com.google.protobuf.InvalidProtocolBufferException;
import com.risingwave.proto.ConnectorServiceProto;

public class SinkWriterRequest implements AutoCloseable {
    private final ConnectorServiceProto.SinkWriterStreamRequest pbRequest;
    private final StreamChunk chunk;
    private final long epoch;
    private final long batchId;
    private final boolean isPb;

    SinkWriterRequest(ConnectorServiceProto.SinkWriterStreamRequest pbRequest) {
        this.pbRequest = pbRequest;
        this.chunk = null;
        this.epoch = 0;
        this.batchId = 0;
        this.isPb = true;
    }

    SinkWriterRequest(StreamChunk chunk, long epoch, long batchId) {
        this.pbRequest = null;
        this.chunk = chunk;
        this.epoch = epoch;
        this.batchId = batchId;
        this.isPb = false;
    }

    public static SinkWriterRequest fromSerializedPayload(byte[] payload) {
        try {
            return new SinkWriterRequest(
                    ConnectorServiceProto.SinkWriterStreamRequest.parseFrom(payload));
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    public static SinkWriterRequest fromStreamChunk(long pointer, long epoch, long batchId) {
        return new SinkWriterRequest(StreamChunk.fromOwnedPointer(pointer), epoch, batchId);
    }

    public ConnectorServiceProto.SinkWriterStreamRequest asPbRequest() {
        if (isPb) {
            return pbRequest;
        } else {
            return ConnectorServiceProto.SinkWriterStreamRequest.newBuilder()
                    .setWriteBatch(
                            ConnectorServiceProto.SinkWriterStreamRequest.WriteBatch.newBuilder()
                                    .setEpoch(epoch)
                                    .setBatchId(batchId)
                                    .setStreamChunkRefPointer(chunk.getPointer())
                                    .build())
                    .build();
        }
    }

    @Override
    public void close() throws Exception {
        if (!isPb && chunk != null) {
            this.chunk.close();
        }
    }
}
