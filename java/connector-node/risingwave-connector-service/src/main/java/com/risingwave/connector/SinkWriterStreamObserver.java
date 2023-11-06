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

import static com.risingwave.connector.SinkUtils.getConnectorName;
import static io.grpc.Status.*;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.*;
import com.risingwave.connector.deserializer.StreamChunkDeserializer;
import com.risingwave.metrics.ConnectorNodeMetrics;
import com.risingwave.metrics.MonitoredRowIterator;
import com.risingwave.proto.ConnectorServiceProto;
import io.grpc.stub.StreamObserver;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkWriterStreamObserver
        implements StreamObserver<ConnectorServiceProto.SinkWriterStreamRequest> {
    private SinkWriter sink;

    private String connectorName;

    private long sinkId;

    private TableSchema tableSchema;

    private boolean finished = false;

    private boolean epochStarted;
    private long currentEpoch;
    private Long currentBatchId;

    private Deserializer deserializer;
    private final StreamObserver<ConnectorServiceProto.SinkWriterStreamResponse> responseObserver;

    private static final Logger LOG = LoggerFactory.getLogger(SinkWriterStreamObserver.class);

    public boolean isInitialized() {
        return sink != null;
    }

    public SinkWriterStreamObserver(
            StreamObserver<ConnectorServiceProto.SinkWriterStreamResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    @Override
    public void onNext(ConnectorServiceProto.SinkWriterStreamRequest sinkTask) {
        if (finished) {
            throw new RuntimeException("unexpected onNext call on a finished writer stream");
        }
        try {
            if (sinkTask.hasStart()) {
                if (isInitialized()) {
                    throw ALREADY_EXISTS
                            .withDescription("Sink is already initialized")
                            .asRuntimeException();
                }
                sinkId = sinkTask.getStart().getSinkParam().getSinkId();
                bindSink(sinkTask.getStart().getSinkParam(), sinkTask.getStart().getFormat());
                responseObserver.onNext(
                        ConnectorServiceProto.SinkWriterStreamResponse.newBuilder()
                                .setStart(
                                        ConnectorServiceProto.SinkWriterStreamResponse.StartResponse
                                                .newBuilder())
                                .build());
            } else if (sinkTask.hasBeginEpoch()) {
                if (!isInitialized()) {
                    throw FAILED_PRECONDITION
                            .withDescription("sink is not initialized, please call start first")
                            .asRuntimeException();
                }
                if (epochStarted && sinkTask.getBeginEpoch().getEpoch() <= currentEpoch) {
                    throw INVALID_ARGUMENT
                            .withDescription(
                                    "invalid epoch: new epoch ID should be larger than current epoch")
                            .asRuntimeException();
                }
                epochStarted = true;
                currentEpoch = sinkTask.getBeginEpoch().getEpoch();
                LOG.debug("Epoch {} started", currentEpoch);
            } else if (sinkTask.hasWriteBatch()) {
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
                ConnectorServiceProto.SinkWriterStreamRequest.WriteBatch batch =
                        sinkTask.getWriteBatch();
                if (batch.getEpoch() != currentEpoch) {
                    throw INVALID_ARGUMENT
                            .withDescription(
                                    "invalid epoch: expected write to epoch "
                                            + currentEpoch
                                            + ", got "
                                            + sinkTask.getWriteBatch().getEpoch())
                            .asRuntimeException();
                }
                if (currentBatchId != null && batch.getBatchId() <= currentBatchId) {
                    throw INVALID_ARGUMENT
                            .withDescription(
                                    "invalid batch ID: expected batch ID to be larger than "
                                            + currentBatchId
                                            + ", got "
                                            + batch.getBatchId())
                            .asRuntimeException();
                }

                try (CloseableIterator<SinkRow> rowIter = deserializer.deserialize(batch)) {
                    sink.write(
                            new MonitoredRowIterator(
                                    rowIter, connectorName, String.valueOf(sinkId)));
                }

                currentBatchId = batch.getBatchId();
                LOG.debug("Batch {} written to epoch {}", currentBatchId, batch.getEpoch());
            } else if (sinkTask.hasBarrier()) {
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
                if (sinkTask.getBarrier().getEpoch() != currentEpoch) {
                    throw INVALID_ARGUMENT
                            .withDescription(
                                    "invalid epoch: expected sync to epoch "
                                            + currentEpoch
                                            + ", got "
                                            + sinkTask.getBarrier().getEpoch())
                            .asRuntimeException();
                }
                boolean isCheckpoint = sinkTask.getBarrier().getIsCheckpoint();
                Optional<ConnectorServiceProto.SinkMetadata> metadata = sink.barrier(isCheckpoint);
                currentEpoch = sinkTask.getBarrier().getEpoch();
                currentBatchId = null;
                LOG.debug("Epoch {} barrier {}", currentEpoch, isCheckpoint);
                if (isCheckpoint) {
                    ConnectorServiceProto.SinkWriterStreamResponse.CommitResponse.Builder builder =
                            ConnectorServiceProto.SinkWriterStreamResponse.CommitResponse
                                    .newBuilder()
                                    .setEpoch(currentEpoch);
                    metadata.ifPresent(builder::setMetadata);
                    responseObserver.onNext(
                            ConnectorServiceProto.SinkWriterStreamResponse.newBuilder()
                                    .setCommit(builder)
                                    .build());
                }
            } else {
                throw INVALID_ARGUMENT.withDescription("invalid sink task").asRuntimeException();
            }
        } catch (Throwable e) {
            LOG.error("sink writer error: ", e);
            cleanup();
            responseObserver.onError(e);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        LOG.error("sink writer finishes with error: ", throwable);
        cleanup();
    }

    @Override
    public void onCompleted() {
        LOG.info("sink writer completed");
        cleanup();
        responseObserver.onCompleted();
    }

    private void cleanup() {
        finished = true;
        if (sink != null) {
            sink.drop();
        }
        ConnectorNodeMetrics.decActiveSinkConnections(connectorName, "node1");
    }

    private void bindSink(
            ConnectorServiceProto.SinkParam sinkParam,
            ConnectorServiceProto.SinkPayloadFormat format) {
        tableSchema = TableSchema.fromProto(sinkParam.getTableSchema());
        String connectorName = getConnectorName(sinkParam);
        SinkFactory sinkFactory = SinkUtils.getSinkFactory(connectorName);
        sink = sinkFactory.createWriter(tableSchema, sinkParam.getPropertiesMap());
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
        this.connectorName = connectorName.toUpperCase();
        ConnectorNodeMetrics.incActiveSinkConnections(connectorName, "node1");
    }
}
