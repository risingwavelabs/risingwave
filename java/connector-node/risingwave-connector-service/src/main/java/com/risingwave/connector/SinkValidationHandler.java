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

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkFactory;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.ConnectorServiceProto.SinkConfig;
import io.grpc.stub.StreamObserver;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
            SinkFactory sinkFactory = SinkUtils.getSinkFactory(sinkConfig.getConnectorType());
            sinkFactory.validate(tableSchema, sinkConfig.getPropertiesMap(), request.getSinkType());

        } catch (IllegalArgumentException e) {
            LOG.error("sink validation failed", e);
            // Extract useful information from the error thrown by Jackson and convert it into a
            // more concise message.
            String errorMessage = e.getLocalizedMessage();
            Pattern missingFieldPattern = Pattern.compile("Missing creator property '([^']*)'");
            Pattern unrecognizedFieldPattern = Pattern.compile("Unrecognized field \"([^\"]*)\"");
            Matcher missingFieldMatcher = missingFieldPattern.matcher(errorMessage);
            Matcher unrecognizedFieldMatcher = unrecognizedFieldPattern.matcher(errorMessage);
            if (missingFieldMatcher.find()) {
                errorMessage = "missing field `" + missingFieldMatcher.group(1) + "`";
            } else if (unrecognizedFieldMatcher.find()) {
                errorMessage = "unknown field `" + unrecognizedFieldMatcher.group(1) + "`";
            }
            responseObserver.onNext(
                    ConnectorServiceProto.ValidateSinkResponse.newBuilder()
                            .setError(
                                    ConnectorServiceProto.ValidationError.newBuilder()
                                            .setErrorMessage(errorMessage)
                                            .build())
                            .build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            LOG.error("sink validation failed", e);
            responseObserver.onNext(
                    ConnectorServiceProto.ValidateSinkResponse.newBuilder()
                            .setError(
                                    ConnectorServiceProto.ValidationError.newBuilder()
                                            .setErrorMessage(e.getMessage())
                                            .build())
                            .build());
            responseObserver.onCompleted();
        }

        responseObserver.onNext(ConnectorServiceProto.ValidateSinkResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
