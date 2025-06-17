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

package com.risingwave.connector.source;

import static com.risingwave.connector.source.SourceValidateHandler.validateResponse;
import static com.risingwave.connector.source.SourceValidateHandler.validateSource;

import com.risingwave.proto.ConnectorServiceProto;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JniSourceValidateHandler {
    static final Logger LOG = LoggerFactory.getLogger(JniSourceValidateHandler.class);

    public static byte[] validate(byte[] validateSourceRequestBytes)
            throws com.google.protobuf.InvalidProtocolBufferException {
        try {
            var request =
                    ConnectorServiceProto.ValidateSourceRequest.parseFrom(
                            validateSourceRequestBytes);
            validateSource(request);
            // validate pass
            return ConnectorServiceProto.ValidateSourceResponse.newBuilder().build().toByteArray();
        } catch (StatusRuntimeException e) {
            LOG.warn("Source validation failed", e);
            return validateResponse(e.getMessage()).toByteArray();
        } catch (Exception e) {
            LOG.error("Internal error on source validation", e);
            return validateResponse("Internal error: " + e.getMessage()).toByteArray();
        }
    }
}
