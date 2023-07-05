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

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.source.common.DbzConnectorConfig;
import com.risingwave.connector.source.common.MySqlValidator;
import com.risingwave.connector.source.common.PostgresValidator;
import com.risingwave.connector.source.common.ValidatorUtils;
import com.risingwave.proto.ConnectorServiceProto;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceValidateHandler {
    static final Logger LOG = LoggerFactory.getLogger(SourceValidateHandler.class);
    private final StreamObserver<ConnectorServiceProto.ValidateSourceResponse> responseObserver;

    public SourceValidateHandler(
            StreamObserver<ConnectorServiceProto.ValidateSourceResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    public void handle(ConnectorServiceProto.ValidateSourceRequest request) {
        try {
            validateSource(request);
            // validate pass
            responseObserver.onNext(
                    ConnectorServiceProto.ValidateSourceResponse.newBuilder().build());
            responseObserver.onCompleted();

        } catch (StatusRuntimeException e) {
            LOG.warn("Source validation failed", e);
            responseObserver.onNext(validateResponse(e.getMessage()));
            responseObserver.onCompleted();

        } catch (Exception e) {
            LOG.error("Internal error on source validation", e);
            responseObserver.onNext(validateResponse("Internal error: " + e.getMessage()));
            responseObserver.onCompleted();
        }
    }

    private ConnectorServiceProto.ValidateSourceResponse validateResponse(String message) {
        return ConnectorServiceProto.ValidateSourceResponse.newBuilder()
                .setError(
                        ConnectorServiceProto.ValidationError.newBuilder()
                                .setErrorMessage(message)
                                .build())
                .build();
    }

    private void ensurePropNotNull(Map<String, String> props, String name) {
        if (!props.containsKey(name)) {
            throw ValidatorUtils.invalidArgument(
                    String.format("'%s' not found, please check the WITH properties", name));
        }
    }

    private void validateSource(ConnectorServiceProto.ValidateSourceRequest request)
            throws Exception {
        var props = request.getPropertiesMap();

        ensurePropNotNull(props, DbzConnectorConfig.HOST);
        ensurePropNotNull(props, DbzConnectorConfig.PORT);
        ensurePropNotNull(props, DbzConnectorConfig.DB_NAME);
        ensurePropNotNull(props, DbzConnectorConfig.TABLE_NAME);
        ensurePropNotNull(props, DbzConnectorConfig.USER);
        ensurePropNotNull(props, DbzConnectorConfig.PASSWORD);

        TableSchema tableSchema = TableSchema.fromProto(request.getTableSchema());
        switch (request.getSourceType()) {
            case POSTGRES:
                ensurePropNotNull(props, DbzConnectorConfig.PG_SCHEMA_NAME);
                try (var validator = new PostgresValidator(props, tableSchema)) {
                    validator.validateAll();
                }
                break;

            case CITUS:
                ensurePropNotNull(props, DbzConnectorConfig.PG_SCHEMA_NAME);
                try (var coordinatorValidator = new PostgresValidator(props, tableSchema)) {
                    coordinatorValidator.validateDistributedTable();
                    coordinatorValidator.validateTable();
                }

                ensurePropNotNull(props, DbzConnectorConfig.DB_SERVERS);
                var workerServers =
                        StringUtils.split(props.get(DbzConnectorConfig.DB_SERVERS), ',');
                for (String workerHost : workerServers) {
                    // set HOST for each worker server
                    props.put(DbzConnectorConfig.HOST, workerHost);
                    try (var workerValidator = new PostgresValidator(props, tableSchema)) {
                        workerValidator.validateDbConfig();
                        workerValidator.validateUserPrivilege();
                    }
                }

                break;
            case MYSQL:
                try (var validator = new MySqlValidator(props, tableSchema)) {
                    validator.validateAll();
                }
                break;
            default:
                LOG.warn("Unknown source type");
                throw ValidatorUtils.invalidArgument("Unknown source type");
        }
    }
}
