package com.risingwave.connector;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkFactory;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.ConnectorServiceProto.SinkConfig;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkValidationHandler {
    private final StreamObserver<ConnectorServiceProto.ValidateSinkResponse> responseObserver;
    static final Logger LOG = LoggerFactory.getLogger(SinkValidationHandler.class);

    public SinkValidationHandler(
            StreamObserver<ConnectorServiceProto.ValidateSinkResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    public void handle(ConnectorServiceProto.ValidateSinkRequest request) {
        try {
            SinkConfig sinkConfig = request.getSinkConfig();
            TableSchema tableSchema = TableSchema.fromProto(sinkConfig.getTableSchema());
            SinkFactory sinkFactory = SinkUtils.getSinkFactory(sinkConfig.getSinkType());
            sinkFactory.validate(tableSchema, sinkConfig.getPropertiesMap());
        } catch (Exception e) {
            LOG.error("sink validation failed", e);
            responseObserver.onNext(
                    ConnectorServiceProto.ValidateSinkResponse.newBuilder()
                            .setError(
                                    ConnectorServiceProto.ValidationError.newBuilder()
                                            .setErrorMessage(e.toString())
                                            .build())
                            .build());
            responseObserver.onCompleted();
        }
    }
}
