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

import static io.grpc.Status.*;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.*;
import com.risingwave.connector.deserializer.StreamChunkDeserializer;
import com.risingwave.metrics.ConnectorNodeMetrics;
import com.risingwave.metrics.MonitoredRowIterator;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.ConnectorServiceProto.SinkConfig;
import com.risingwave.proto.ConnectorServiceProto.SinkResponse.StartResponse;
import com.risingwave.proto.ConnectorServiceProto.SinkResponse.SyncResponse;
import com.risingwave.proto.ConnectorServiceProto.SinkResponse.WriteResponse;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkStreamObserver implements StreamObserver<ConnectorServiceProto.SinkStreamRequest> {
    private SinkBase sink;

    private String sinkType;

    private long sinkId;

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
                bindSink(sinkTask.getStart().getSinkConfig(), sinkTask.getStart().getFormat());
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

                try (CloseableIterator<SinkRow> rowIter =
                        deserializer.deserialize(sinkTask.getWrite())) {
                    sink.write(new MonitoredRowIterator(rowIter, sinkType, String.valueOf(sinkId)));
                }

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
        cleanup();
        responseObserver.onError(throwable);
    }

    @Override
    public void onCompleted() {
        LOG.debug("sink task completed");
        cleanup();
        responseObserver.onCompleted();
    }

    private void cleanup() {
        if (sink != null) {
            sink.drop();
        }
    }

    private void bindSink(SinkConfig sinkConfig, ConnectorServiceProto.SinkPayloadFormat format) {
        tableSchema = TableSchema.fromProto(sinkConfig.getTableSchema());
        SinkFactory sinkFactory = SinkUtils.getSinkFactory(sinkConfig.getConnectorType());
        sink = sinkFactory.create(tableSchema, sinkConfig.getPropertiesMap());
        switch (format) {
            case FORMAT_UNSPECIFIED:
            case UNRECOGNIZED:
                throw INVALID_ARGUMENT
                        .withDescription("should specify payload format in request")
                        .asRuntimeException();
            case JSON:
                deserializer = new JsonDeserializer(tableSchema);
                break;
            case STREAM_CHUNK:
                deserializer = new StreamChunkDeserializer(tableSchema);
                break;
        }
        sinkType = sinkConfig.getConnectorType();
        sinkId = sinkConfig.getSinkId();
        ConnectorNodeMetrics.incActiveSinkConnections(sinkConfig.getConnectorType(), "node1");
    }
}
