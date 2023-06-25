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

package com.risingwave.connector.source;

import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.connector.source.core.SourceHandlerFactory;
import com.risingwave.proto.ConnectorServiceProto;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.Map;

public class SourceRequestHandler {
    private Map<Long, String> replicationSlotMap;
    private final StreamObserver<ConnectorServiceProto.GetEventStreamResponse> responseObserver;

    public SourceRequestHandler(
            Map<Long, String> replicationSlotMap,
            StreamObserver<ConnectorServiceProto.GetEventStreamResponse> responseObserver) {
        this.replicationSlotMap = replicationSlotMap;
        this.responseObserver = responseObserver;
    }

    public void handle(ConnectorServiceProto.GetEventStreamRequest request) {
        var handler =
                SourceHandlerFactory.createSourceHandler(
                        SourceTypeE.valueOf(request.getSourceType()),
                        request.getSourceId(),
                        request.getStartOffset(),
                        request.getPropertiesMap(),
                        replicationSlotMap);
        handler.startSource(
                (ServerCallStreamObserver<ConnectorServiceProto.GetEventStreamResponse>)
                        responseObserver);
    }
}
