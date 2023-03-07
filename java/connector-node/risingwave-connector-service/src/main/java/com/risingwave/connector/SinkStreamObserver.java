package com.risingwave.connector;

import static io.grpc.Status.*;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkBase;
import com.risingwave.connector.api.sink.SinkFactory;
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.metrics.ConnectorNodeMetrics;
import com.risingwave.metrics.MonitoredRowIterator;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.ConnectorServiceProto.SinkConfig;
import com.risingwave.proto.ConnectorServiceProto.SinkResponse.StartResponse;
import com.risingwave.proto.ConnectorServiceProto.SinkResponse.SyncResponse;
import com.risingwave.proto.ConnectorServiceProto.SinkResponse.WriteResponse;
import io.grpc.stub.StreamObserver;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkStreamObserver implements StreamObserver<ConnectorServiceProto.SinkStreamRequest> {
    private SinkBase sink;

    private TableSchema tableSchema;

    private boolean epochStarted;
    private long currentEpoch;
    private Long currentBatchId;

    private Deserializer deserializer;
    private final StreamObserver<ConnectorServiceProto.SinkResponse> responseObserver;

    private static final Logger LOG = LoggerFactory.getLogger(SinkStreamObserver.class);

    public boolean isInitialized() {
        return sink != null;
    }

    public SinkStreamObserver(StreamObserver<ConnectorServiceProto.SinkResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    @Override
    public void onNext(ConnectorServiceProto.SinkStreamRequest sinkTask) {
        try {
            if (sinkTask.hasStart()) {
                if (isInitialized()) {
                    throw ALREADY_EXISTS
                            .withDescription("Sink is already initialized")
                            .asRuntimeException();
                }
                bindSink(sinkTask.getStart().getSinkConfig());
                LOG.debug("Sink initialized");
                responseObserver.onNext(
                        ConnectorServiceProto.SinkResponse.newBuilder()
                                .setStart(StartResponse.newBuilder().build())
                                .build());
            } else if (sinkTask.hasStartEpoch()) {
                if (!isInitialized()) {
                    throw FAILED_PRECONDITION
                            .withDescription("sink is not initialized, please call start first")
                            .asRuntimeException();
                }
                if (epochStarted && sinkTask.getStartEpoch().getEpoch() <= currentEpoch) {
                    throw INVALID_ARGUMENT
                            .withDescription(
                                    "invalid epoch: new epoch ID should be larger than current epoch")
                            .asRuntimeException();
                }
                epochStarted = true;
                currentEpoch = sinkTask.getStartEpoch().getEpoch();
                LOG.debug("Epoch {} started", currentEpoch);
                responseObserver.onNext(
                        ConnectorServiceProto.SinkResponse.newBuilder()
                                .setStartEpoch(
                                        ConnectorServiceProto.SinkResponse.StartEpochResponse
                                                .newBuilder()
                                                .setEpoch(currentEpoch)
                                                .build())
                                .build());
            } else if (sinkTask.hasWrite()) {
                if (!isInitialized()) {
                    throw FAILED_PRECONDITION
                            .withDescription("Sink is not initialized. Invoke `CreateSink` first.")
                            .asRuntimeException();
                }
                if (!epochStarted) {
                    throw FAILED_PRECONDITION
                            .withDescription("Epoch is not started. Invoke `StartEpoch` first.")
                            .asRuntimeException();
                }
                if (sinkTask.getWrite().getEpoch() != currentEpoch) {
                    throw INVALID_ARGUMENT
                            .withDescription(
                                    "invalid epoch: expected write to epoch "
                                            + currentEpoch
                                            + ", got "
                                            + sinkTask.getWrite().getEpoch())
                            .asRuntimeException();
                }
                if (currentBatchId != null && sinkTask.getWrite().getBatchId() <= currentBatchId) {
                    throw INVALID_ARGUMENT
                            .withDescription(
                                    "invalid batch ID: expected batch ID to be larger than "
                                            + currentBatchId
                                            + ", got "
                                            + sinkTask.getWrite().getBatchId())
                            .asRuntimeException();
                }

                Iterator<SinkRow> rows;
                switch (sinkTask.getWrite().getPayloadCase()) {
                    case JSON_PAYLOAD:
                        if (deserializer == null) {
                            deserializer = new JsonDeserializer(tableSchema);
                        }

                        if (deserializer instanceof JsonDeserializer) {
                            rows = deserializer.deserialize(sinkTask.getWrite().getJsonPayload());
                        } else {
                            throw INTERNAL.withDescription(
                                            "invalid payload type: expected JSON, got "
                                                    + deserializer.getClass().getName())
                                    .asRuntimeException();
                        }
                        break;
                    default:
                        throw INVALID_ARGUMENT
                                .withDescription("invalid payload type")
                                .asRuntimeException();
                }
                sink.write(new MonitoredRowIterator(rows));

                currentBatchId = sinkTask.getWrite().getBatchId();
                LOG.debug(
                        "Batch {} written to epoch {}",
                        currentBatchId,
                        sinkTask.getWrite().getEpoch());
                responseObserver.onNext(
                        ConnectorServiceProto.SinkResponse.newBuilder()
                                .setWrite(WriteResponse.newBuilder().build())
                                .build());
            } else if (sinkTask.hasSync()) {
                if (!isInitialized()) {
                    throw FAILED_PRECONDITION
                            .withDescription("Sink is not initialized. Invoke `Start` first.")
                            .asRuntimeException();
                }
                if (!epochStarted) {
                    throw FAILED_PRECONDITION
                            .withDescription("Epoch is not started. Invoke `StartEpoch` first.")
                            .asRuntimeException();
                }
                if (sinkTask.getSync().getEpoch() != currentEpoch) {
                    throw INVALID_ARGUMENT
                            .withDescription(
                                    "invalid epoch: expected sync to epoch "
                                            + currentEpoch
                                            + ", got "
                                            + sinkTask.getSync().getEpoch())
                            .asRuntimeException();
                }
                sink.sync();
                currentEpoch = sinkTask.getSync().getEpoch();
                LOG.debug("Epoch {} synced", currentEpoch);
                responseObserver.onNext(
                        ConnectorServiceProto.SinkResponse.newBuilder()
                                .setSync(SyncResponse.newBuilder().setEpoch(currentEpoch).build())
                                .build());
            } else {
                throw INVALID_ARGUMENT.withDescription("invalid sink task").asRuntimeException();
            }
        } catch (Exception e) {
            LOG.error("sink task error: ", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        LOG.error("sink task error: ", throwable);
        if (sink != null) {
            sink.drop();
        }
        responseObserver.onError(throwable);
    }

    @Override
    public void onCompleted() {
        LOG.debug("sink task completed");
        if (sink != null) {
            sink.drop();
        }
        responseObserver.onCompleted();
    }

    private void bindSink(SinkConfig sinkConfig) {
        tableSchema = TableSchema.fromProto(sinkConfig.getTableSchema());
        SinkFactory sinkFactory = SinkUtils.getSinkFactory(sinkConfig.getSinkType());
        sink = sinkFactory.create(tableSchema, sinkConfig.getPropertiesMap());
        ConnectorNodeMetrics.incActiveConnections(sinkConfig.getSinkType(), "node1");
    }
}
