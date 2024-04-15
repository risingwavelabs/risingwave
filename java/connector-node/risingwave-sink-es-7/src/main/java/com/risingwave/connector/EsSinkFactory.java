// Copyright 2024 RisingWave Labs
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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkFactory;
import com.risingwave.connector.api.sink.SinkWriter;
import com.risingwave.connector.api.sink.SinkWriterV1;
import com.risingwave.proto.Catalog;
import com.risingwave.proto.Data;
import io.grpc.Status;
import java.io.IOException;
import java.util.Map;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EsSinkFactory implements SinkFactory {
    private static final Logger LOG = LoggerFactory.getLogger(EsSinkFactory.class);

    public SinkWriter createWriter(TableSchema tableSchema, Map<String, String> tableProperties) {
        ObjectMapper mapper = new ObjectMapper();
        EsSinkConfig config = mapper.convertValue(tableProperties, EsSinkConfig.class);
        return new SinkWriterV1.Adapter(new EsSink(config, tableSchema));
    }

    @Override
    public void validate(
            TableSchema tableSchema,
            Map<String, String> tableProperties,
            Catalog.SinkType sinkType) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES, true);
        EsSinkConfig config = mapper.convertValue(tableProperties, EsSinkConfig.class);

        // 1. check url
        HttpHost host;
        try {
            host = HttpHost.create(config.getUrl());
        } catch (IllegalArgumentException e) {
            throw Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException();
        }
        if (config.getIndexColumn() != null) {
            Data.DataType.TypeName typeName = tableSchema.getColumnType(config.getIndexColumn());
            if (typeName == null) {
                throw Status.INVALID_ARGUMENT
                        .withDescription(
                                "Index column " + config.getIndexColumn() + " not found in schema")
                        .asRuntimeException();
            }
            if (!typeName.equals(Data.DataType.TypeName.VARCHAR)) {
                throw Status.INVALID_ARGUMENT
                        .withDescription(
                                "Index column must be of type String, but found " + typeName)
                        .asRuntimeException();
            }
            if (config.getIndex() != null) {
                throw Status.INVALID_ARGUMENT
                        .withDescription("index and index_column cannot be set at the same time")
                        .asRuntimeException();
            }
        } else {
            if (config.getIndex() == null) {
                throw Status.INVALID_ARGUMENT
                        .withDescription("index or index_column must be set")
                        .asRuntimeException();
            }
        }

        RestHighLevelClientAdapter client;
        // 2. check connection
        try {
            boolean isConnected;
            if (config.getConnector().equals("elasticsearch")) {
                client = new ElasticRestHighLevelClientAdapter(host, config);
                isConnected = client.ping(RequestOptions.DEFAULT);
            } else if (config.getConnector().equals("opensearch")) {
                client = new OpensearchRestHighLevelClientAdapter(host, config);
                isConnected = client.ping(org.opensearch.client.RequestOptions.DEFAULT);
            } else {
                throw new RuntimeException("Sink type must be elasticsearch or opensearch");
            }
            if (!isConnected) {
                throw Status.INVALID_ARGUMENT
                        .withDescription("Cannot connect to " + config.getUrl())
                        .asRuntimeException();
            }
        } catch (Exception e) {
            throw Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException();
        }

        // 3. close client
        try {
            client.close();
        } catch (IOException e) {
            throw Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException();
        }
    }
}
