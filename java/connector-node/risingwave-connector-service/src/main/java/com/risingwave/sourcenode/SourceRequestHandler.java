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

package com.risingwave.sourcenode;

import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.metrics.ConnectorNodeMetrics;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.sourcenode.common.DbzConnectorConfig;
import com.risingwave.sourcenode.core.SourceHandlerFactory;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceRequestHandler {
    private final StreamObserver<ConnectorServiceProto.GetEventStreamResponse> responseObserver;
    static final Logger LOG = LoggerFactory.getLogger(SourceRequestHandler.class);

    public SourceRequestHandler(
            StreamObserver<ConnectorServiceProto.GetEventStreamResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    public void handle(ConnectorServiceProto.GetEventStreamRequest request) {
        switch (request.getRequestCase()) {
            case START:
                var startRequest = request.getStart();
                try {
                    var handler =
                            SourceHandlerFactory.createSourceHandler(
                                    SourceTypeE.valueOf(startRequest.getSourceType()),
                                    startRequest.getSourceId(),
                                    startRequest.getStartOffset(),
                                    startRequest.getPropertiesMap());
                    ConnectorNodeMetrics.incActiveSourceConnections(
                            startRequest.getSourceType().toString(),
                            startRequest.getPropertiesMap().get(DbzConnectorConfig.HOST));
                    handler.startSource(
                            (ServerCallStreamObserver<ConnectorServiceProto.GetEventStreamResponse>)
                                    responseObserver);
                    ConnectorNodeMetrics.decActiveSourceConnections(
                            startRequest.getSourceType().toString(),
                            startRequest.getPropertiesMap().get(DbzConnectorConfig.HOST));
                } catch (Throwable t) {
                    LOG.error("failed to start source", t);
                    responseObserver.onError(t);
                }
                break;
            case REQUEST_NOT_SET:
                LOG.warn("request not set");
                responseObserver.onCompleted();
                break;
        }
    }
}
