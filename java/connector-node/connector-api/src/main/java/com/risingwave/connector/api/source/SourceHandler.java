package com.risingwave.connector.api.source;

import com.risingwave.proto.ConnectorServiceProto;
import io.grpc.stub.ServerCallStreamObserver;

/** Handler for RPC request */
public interface SourceHandler {
    void handle(
            ServerCallStreamObserver<ConnectorServiceProto.GetEventStreamResponse>
                    responseObserver);
}
