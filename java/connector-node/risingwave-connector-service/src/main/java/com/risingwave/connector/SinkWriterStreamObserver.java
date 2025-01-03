// Copyright 2025 RisingWave Labs
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
import com.risingwave.metrics.MonitoredRowIterable;
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
    private Long currentEpoch;
    private Long currentBatchId;

    private Deserializer deserializer;
    private final StreamObserver<ConnectorServiceProto.SinkWriterStreamResponse> responseObserver;

    private static final Logger LOG = LoggerFactory.getLogger(SinkWriterStreamObserver.class);

    private boolean isInitialized() {
        return sink != null;
    }

    private void receiveEpoch(long epoch, String context) {
        if (!isInitialized()) {
            throw FAILED_PRECONDITION
                    .withDescription("Sink is not initialized. Invoke `CreateSink` first.")
                    .asRuntimeException();
        }
        if (!epochStarted) {
            if (currentEpoch != null && epoch <= currentEpoch) {
                throw FAILED_PRECONDITION
                        .withDescription(
                                String.format(
                                        "in [%s], expect a new epoch higher than current epoch %s but got %s",
                                        context, currentEpoch, epoch))
                        .asRuntimeException();
            }
            sink.beginEpoch(epoch);
            epochStarted = true;
            currentEpoch = epoch;
        } else {
            if (epoch != currentEpoch) {
                throw INVALID_ARGUMENT
                        .withDescription(
                                String.format(
                                        "in [%s] invalid epoch: expected write to epoch %s, got %s",
                                        context, currentEpoch, epoch))
                        .asRuntimeException();
            }
        }
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
                bindSink(sinkTask.getStart());
                currentEpoch = null;
                currentBatchId = null;
                epochStarted = false;
                responseObserver.onNext(
                        ConnectorServiceProto.SinkWriterStreamResponse.newBuilder()
                                .setStart(
                                        ConnectorServiceProto.SinkWriterStreamResponse.StartResponse
                                                .newBuilder())
                                .build());
            } else if (sinkTask.hasWriteBatch()) {
                ConnectorServiceProto.SinkWriterStreamRequest.WriteBatch batch =
                        sinkTask.getWriteBatch();
                receiveEpoch(batch.getEpoch(), "WriteBatch");
                if (currentBatchId != null && batch.getBatchId() <= currentBatchId) {
                    throw INVALID_ARGUMENT
                            .withDescription(
                                    "invalid batch ID: expected batch ID to be larger than "
                                            + currentBatchId
                                            + ", got "
                                            + batch.getBatchId())
                            .asRuntimeException();
                }

                boolean batchWritten;

                try (CloseableIterable<SinkRow> rowIter = deserializer.deserialize(batch)) {
                    batchWritten =
                            sink.write(
                                    new MonitoredRowIterable(
                                            rowIter, connectorName, String.valueOf(sinkId)));
                }

                currentBatchId = batch.getBatchId();

                if (batchWritten) {
                    responseObserver.onNext(
                            ConnectorServiceProto.SinkWriterStreamResponse.newBuilder()
                                    .setBatch(
                                            ConnectorServiceProto.SinkWriterStreamResponse
                                                    .BatchWrittenResponse.newBuilder()
                                                    .setEpoch(currentEpoch)
                                                    .setBatchId(currentBatchId)
                                                    .build())
                                    .build());
                }

                LOG.debug("Batch {} written to epoch {}", currentBatchId, batch.getEpoch());
            } else if (sinkTask.hasBarrier()) {
                ConnectorServiceProto.SinkWriterStreamRequest.Barrier barrier =
                        sinkTask.getBarrier();
                receiveEpoch(barrier.getEpoch(), "Barrier");
                boolean isCheckpoint = barrier.getIsCheckpoint();
                Optional<ConnectorServiceProto.SinkMetadata> metadata = sink.barrier(isCheckpoint);
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
                currentBatchId = null;
                epochStarted = false;
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
        LOG.debug("sink writer completed");
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

    private void bindSink(ConnectorServiceProto.SinkWriterStreamRequest.StartSink startSink) {
        var sinkParam = startSink.getSinkParam();
        tableSchema = TableSchema.fromProto(startSink.getPayloadSchema());
        String connectorName = getConnectorName(sinkParam);
        SinkFactory sinkFactory = SinkUtils.getSinkFactory(connectorName);
        sink = sinkFactory.createWriter(tableSchema, sinkParam.getPropertiesMap());
        deserializer = new StreamChunkDeserializer(tableSchema);
        this.connectorName = connectorName.toUpperCase();
        ConnectorNodeMetrics.incActiveSinkConnections(connectorName, "node1");
    }
}
