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

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkFactory;
import com.risingwave.proto.ConnectorServiceProto;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JniSinkValidationHandler {
    static final Logger LOG = LoggerFactory.getLogger(SinkValidationHandler.class);

    public static byte[] validate(byte[] validateSinkRequestBytes)
            throws com.google.protobuf.InvalidProtocolBufferException {
        try {
            var request =
                    ConnectorServiceProto.ValidateSinkRequest.parseFrom(validateSinkRequestBytes);

            // For jni.rs
            java.lang.Thread.currentThread()
                    .setContextClassLoader(java.lang.ClassLoader.getSystemClassLoader());

            ConnectorServiceProto.SinkParam sinkParam = request.getSinkParam();
            TableSchema tableSchema = TableSchema.fromProto(sinkParam.getTableSchema());
            String connectorName = getConnectorName(request.getSinkParam());
            SinkFactory sinkFactory = SinkUtils.getSinkFactory(connectorName);
            sinkFactory.validate(
                    tableSchema, sinkParam.getPropertiesMap(), sinkParam.getSinkType());

            return ConnectorServiceProto.ValidateSinkResponse.newBuilder().build().toByteArray();
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
            return ConnectorServiceProto.ValidateSinkResponse.newBuilder()
                    .setError(
                            ConnectorServiceProto.ValidationError.newBuilder()
                                    .setErrorMessage(errorMessage)
                                    .build())
                    .build()
                    .toByteArray();
        } catch (Exception e) {
            LOG.error("sink validation failed", e);
            return ConnectorServiceProto.ValidateSinkResponse.newBuilder()
                    .setError(
                            ConnectorServiceProto.ValidationError.newBuilder()
                                    .setErrorMessage(e.getMessage())
                                    .build())
                    .build()
                    .toByteArray();
        }
    }
}
