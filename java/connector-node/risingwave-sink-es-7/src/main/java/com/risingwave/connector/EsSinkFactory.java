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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkFactory;
import com.risingwave.connector.api.sink.SinkWriter;
import com.risingwave.connector.api.sink.SinkWriterV1;
import com.risingwave.proto.Catalog;
import com.risingwave.proto.Data;
import io.grpc.Status;
import java.util.Map;
import org.apache.http.HttpHost;
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
        if (config.getRoutingColumn() != null) {
            checkColumn(config.getRoutingColumn(), tableSchema, Data.DataType.TypeName.VARCHAR);
        }
        if (config.getIndexColumn() != null) {
            checkColumn(config.getIndexColumn(), tableSchema, Data.DataType.TypeName.VARCHAR);
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

        // 2. check connection
        try {
            if (config.getConnector().equals("elasticsearch_v1")) {
                ElasticRestHighLevelClientAdapter esClient =
                        new ElasticRestHighLevelClientAdapter(host, config);
                if (!esClient.ping(org.elasticsearch.client.RequestOptions.DEFAULT)) {
                    throw Status.INVALID_ARGUMENT
                            .withDescription("Cannot connect to " + config.getUrl())
                            .asRuntimeException();
                }
                esClient.close();
            } else if (config.getConnector().equals("opensearch_v1")) {
                OpensearchRestHighLevelClientAdapter opensearchClient =
                        new OpensearchRestHighLevelClientAdapter(host, config);
                if (!opensearchClient.ping(org.opensearch.client.RequestOptions.DEFAULT)) {
                    throw Status.INVALID_ARGUMENT
                            .withDescription("Cannot connect to " + config.getUrl())
                            .asRuntimeException();
                }
                opensearchClient.close();
            } else {
                throw new RuntimeException("Sink type must be elasticsearch or opensearch");
            }
        } catch (Exception e) {
            throw Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException();
        }
    }

    private void checkColumn(
            String column, TableSchema tableSchema, Data.DataType.TypeName typeName) {
        Data.DataType.TypeName columnType = tableSchema.getColumnType(column);
        if (columnType == null) {
            throw Status.INVALID_ARGUMENT
                    .withDescription("Column " + column + " not found in schema")
                    .asRuntimeException();
        }
        if (!columnType.equals(typeName)) {
            throw Status.INVALID_ARGUMENT
                    .withDescription(
                            "Column "
                                    + column
                                    + " must be of type "
                                    + typeName
                                    + ", but found "
                                    + columnType)
                    .asRuntimeException();
        }
    }
}
