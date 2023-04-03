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

import com.risingwave.proto.ConnectorServiceGrpc;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.sourcenode.SourceRequestHandler;
import io.grpc.stub.StreamObserver;

public class ConnectorServiceImpl extends ConnectorServiceGrpc.ConnectorServiceImplBase {

    @Override
    public StreamObserver<ConnectorServiceProto.SinkStreamRequest> sinkStream(
            StreamObserver<ConnectorServiceProto.SinkResponse> responseObserver) {
        return new SinkStreamObserver(responseObserver);
    }

    @Override
    public void validateSink(
            ConnectorServiceProto.ValidateSinkRequest request,
            StreamObserver<ConnectorServiceProto.ValidateSinkResponse> responseObserver) {
        new SinkValidationHandler(responseObserver).handle(request);
    }

    @Override
    public void getEventStream(
            ConnectorServiceProto.GetEventStreamRequest request,
            StreamObserver<ConnectorServiceProto.GetEventStreamResponse> responseObserver) {
        new SourceRequestHandler(responseObserver).handle(request);
    }

    @Override
    public void validateSource(
            ConnectorServiceProto.ValidateSourceRequest request,
            StreamObserver<ConnectorServiceProto.ValidateSourceResponse> responseObserver) {
        new SourceValidateHandler(responseObserver).handle(request);
    }
}
