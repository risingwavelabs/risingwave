package com.risingwave.connector;

import com.risingwave.proto.ConnectorServiceGrpc;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.sourcenode.SourceRequestHandler;
import io.grpc.stub.StreamObserver;

public class ConnectorServiceImpl extends ConnectorServiceGrpc.ConnectorServiceImplBase {

    @Override
    public StreamObserver<ConnectorServiceProto.SinkStreamRequest> sinkStream(
            StreamObserver<ConnectorServiceProto.SinkResponse> responseObserver) {
        return new SinkStreamObserver(responseObserver);
    }

    @Override
    public void getEventStream(
            ConnectorServiceProto.GetEventStreamRequest request,
            StreamObserver<ConnectorServiceProto.GetEventStreamResponse> responseObserver) {
        new SourceRequestHandler(responseObserver).handle(request);
    }
}
