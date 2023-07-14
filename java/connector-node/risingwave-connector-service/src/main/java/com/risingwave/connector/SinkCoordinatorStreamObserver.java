package com.risingwave.connector;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkCoordinator;
import com.risingwave.connector.api.sink.SinkFactory;
import com.risingwave.proto.ConnectorServiceProto;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

public class SinkCoordinatorStreamObserver
        implements StreamObserver<ConnectorServiceProto.SinkCoordinatorStreamRequest> {

    private final StreamObserver<ConnectorServiceProto.SinkCoordinatorStreamResponse>
            responseObserver;
    private SinkCoordinator coordinator = null;

    public SinkCoordinatorStreamObserver(
            StreamObserver<ConnectorServiceProto.SinkCoordinatorStreamResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    @Override
    public void onNext(ConnectorServiceProto.SinkCoordinatorStreamRequest request) {
        if (request.hasStart()) {
            if (this.coordinator != null) {
                throw Status.INVALID_ARGUMENT
                        .withDescription("sink coordinator has started")
                        .asRuntimeException();
            }
            ConnectorServiceProto.SinkParam param = request.getStart().getParam();
            SinkFactory factory = SinkUtils.getSinkFactory(SinkUtils.getConnectorName(param));
            TableSchema schema =
                    TableSchema.fromProto(request.getStart().getParam().getTableSchema());
            this.coordinator =
                    factory.createCoordinator(
                            schema, request.getStart().getParam().getPropertiesMap());
            this.responseObserver.onNext(
                    ConnectorServiceProto.SinkCoordinatorStreamResponse.newBuilder()
                            .setStart(
                                    ConnectorServiceProto.SinkCoordinatorStreamResponse
                                            .StartResponse.newBuilder()
                                            .build())
                            .build());
        } else if (request.hasCommit()) {
            if (this.coordinator == null) {
                throw Status.INVALID_ARGUMENT
                        .withDescription("commit without initialize the coordinator")
                        .asRuntimeException();
            }
            this.coordinator.commit(
                    request.getCommit().getEpoch(), request.getCommit().getMetadataList());
            this.responseObserver.onNext(
                    ConnectorServiceProto.SinkCoordinatorStreamResponse.newBuilder()
                            .setCommit(
                                    ConnectorServiceProto.SinkCoordinatorStreamResponse
                                            .CommitResponse.newBuilder()
                                            .build())
                            .build());

        } else {
            throw Status.INVALID_ARGUMENT
                    .withDescription(String.format("invalid argument: %s", request))
                    .asRuntimeException();
        }
    }

    @Override
    public void onError(Throwable throwable) {
        cleanUp();
        this.responseObserver.onError(
                Status.INTERNAL.withDescription("stopped with error").asException());
    }

    @Override
    public void onCompleted() {
        cleanUp();
        this.responseObserver.onCompleted();
    }

    void cleanUp() {
        if (this.coordinator != null) {
            this.coordinator.drop();
        }
    }
}
