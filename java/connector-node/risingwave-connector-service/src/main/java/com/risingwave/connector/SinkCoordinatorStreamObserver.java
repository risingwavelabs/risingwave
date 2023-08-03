/*
 * Copyright 2023 RisingWave Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.risingwave.connector;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkCoordinator;
import com.risingwave.connector.api.sink.SinkFactory;
import com.risingwave.proto.ConnectorServiceProto;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkCoordinatorStreamObserver
        implements StreamObserver<ConnectorServiceProto.SinkCoordinatorStreamRequest> {
    private static final Logger LOG = LoggerFactory.getLogger(SinkCoordinatorStreamObserver.class);

    private final StreamObserver<ConnectorServiceProto.SinkCoordinatorStreamResponse>
            responseObserver;
    private SinkCoordinator coordinator = null;

    public SinkCoordinatorStreamObserver(
            StreamObserver<ConnectorServiceProto.SinkCoordinatorStreamResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    @Override
    public void onNext(ConnectorServiceProto.SinkCoordinatorStreamRequest request) {
        try {
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
        } catch (Exception ex) {
            String errString =
                    String.format("get error when processing coordinator stream: %s", ex);
            LOG.error(errString);
            this.responseObserver.onError(Status.INTERNAL.withDescription(errString).asException());
        }
    }

    @Override
    public void onError(Throwable throwable) {
        LOG.error("coordinator stream receive error %s", throwable);
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
