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
import static io.grpc.Status.INVALID_ARGUMENT;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.*;
import com.risingwave.connector.deserializer.StreamChunkDeserializer;
import com.risingwave.java.binding.Binding;
import com.risingwave.metrics.ConnectorNodeMetrics;
import com.risingwave.metrics.MonitoredRowIterator;
import com.risingwave.proto.ConnectorServiceProto;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JniSinkWriterHandler {
    private static final Logger LOG = LoggerFactory.getLogger(JniSinkWriterHandler.class);
    private SinkWriter sink;

    private String connectorName;

    private long sinkId;

    private TableSchema tableSchema;

    private boolean epochStarted;
    private long currentEpoch;
    private Long currentBatchId;

    private Deserializer deserializer;

    private long requestRxPtr;

    private long responseTxPtr;

    public boolean isInitialized() {
        return sink != null;
    }

    public JniSinkWriterHandler(long requestRxPtr, long responseTxPtr) {
        this.requestRxPtr = requestRxPtr;
        this.responseTxPtr = responseTxPtr;
    }

    public static void runJniSinkWriterThread(long requestRxPtr, long responseTxPtr)
            throws com.google.protobuf.InvalidProtocolBufferException {
        // For jni.rs
        java.lang.Thread.currentThread()
                .setContextClassLoader(java.lang.ClassLoader.getSystemClassLoader());

        JniSinkWriterHandler handler = new JniSinkWriterHandler(requestRxPtr, responseTxPtr);

        byte[] requestBytes;
        while ((requestBytes = Binding.recvSinkWriterRequestFromChannel(handler.requestRxPtr))
                != null) {
            var request = ConnectorServiceProto.SinkWriterStreamRequest.parseFrom(requestBytes);
            if (!handler.onNext(request)) {
                handler.cleanup();
                return;
            }
        }
        LOG.info("end of runJniSinkWriterThread");
    }

    public boolean onNext(ConnectorServiceProto.SinkWriterStreamRequest sinkTask) {
        try {
            if (sinkTask.hasStart()) {
                if (isInitialized()) {
                    throw ALREADY_EXISTS
                            .withDescription("Sink is already initialized")
                            .asRuntimeException();
                }
                LOG.debug("sinkTask received");
                sinkId = sinkTask.getStart().getSinkParam().getSinkId();
                bindSink(sinkTask.getStart().getSinkParam(), sinkTask.getStart().getFormat());
                return Binding.sendSinkWriterResponseToChannel(
                        this.responseTxPtr,
                        ConnectorServiceProto.SinkWriterStreamResponse.newBuilder()
                                .setStart(
                                        ConnectorServiceProto.SinkWriterStreamResponse.StartResponse
                                                .newBuilder())
                                .build()
                                .toByteArray());
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
                return Binding.sendSinkWriterResponseToChannel(this.responseTxPtr, null);
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
                return Binding.sendSinkWriterResponseToChannel(this.responseTxPtr, null);
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
                LOG.debug("Epoch {} barrier {}", currentEpoch, isCheckpoint);
                if (isCheckpoint) {
                    ConnectorServiceProto.SinkWriterStreamResponse.CommitResponse.Builder builder =
                            ConnectorServiceProto.SinkWriterStreamResponse.CommitResponse
                                    .newBuilder()
                                    .setEpoch(currentEpoch);
                    if (metadata.isPresent()) {
                        builder.setMetadata(metadata.get());
                    }
                    return Binding.sendSinkWriterResponseToChannel(
                            this.responseTxPtr,
                            ConnectorServiceProto.SinkWriterStreamResponse.newBuilder()
                                    .setCommit(builder)
                                    .build()
                                    .toByteArray());
                } else {
                    return Binding.sendSinkWriterResponseToChannel(this.responseTxPtr, null);
                }
            } else {
                throw INVALID_ARGUMENT.withDescription("invalid sink task").asRuntimeException();
            }
        } catch (Exception e) {
            LOG.error("sink task error: ", e);
            return false;
        }
    }

    private void cleanup() {
        if (sink != null) {
            sink.drop();
            ConnectorNodeMetrics.decActiveSinkConnections(connectorName, "node1");
        }
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
