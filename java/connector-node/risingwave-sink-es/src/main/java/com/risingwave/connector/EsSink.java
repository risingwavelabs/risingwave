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
import com.risingwave.connector.api.sink.SinkBase;
import com.risingwave.connector.api.sink.SinkRow;
import io.grpc.Status;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Note:
 *
 * 1. TODO: If no primary key is defined on the DDL, the connector can only operate in append
 * mode for exchanging INSERT only messages with external system.
 *
 * 2. Currently, index is fixed.
 *
 * 3. Possible settings from flink:
 * https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/table/elasticsearch/
 *
 * 4. bulkprocessor and high-level-client are deprecated in es 8 java api.
 */
public class EsSink extends SinkBase {
    private static final Logger LOG = LoggerFactory.getLogger(EsSink.class);
    private static final String ERROR_REPORT_TEMPLATE = "Error when exec %s, message %s";

    private final EsSinkConfig config;
    private final BulkProcessor bulkProcessor;
    private final RestHighLevelClient client;
    // For bulk listener
    private final List<Integer> primaryKeyIndexes;

    public EsSink(EsSinkConfig config, TableSchema tableSchema) {
        super(tableSchema);
        HttpHost host;
        try {
            host = HttpHost.create(config.getUrl());
        } catch (IllegalArgumentException e) {
            throw Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException();
        }

        this.config = config;
        this.client =
                new RestHighLevelClient(
                        configureRestClientBuilder(RestClient.builder(host), config));
        // Test connection
        try {
            boolean isConnected = this.client.ping(RequestOptions.DEFAULT);
            if (!isConnected) {
                throw Status.INVALID_ARGUMENT
                        .withDescription("Cannot connect to " + config.getUrl())
                        .asRuntimeException();
            }
        } catch (Exception e) {
            throw Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException();
        }
        this.bulkProcessor = createBulkProcessor();

        primaryKeyIndexes = new ArrayList<Integer>();
        for (String primaryKey : tableSchema.getPrimaryKeys()) {
            primaryKeyIndexes.add(tableSchema.getColumnIndex(primaryKey));
        }
    }

    private static RestClientBuilder configureRestClientBuilder(
            RestClientBuilder builder, EsSinkConfig config) {
        // Possible config:
        // 1. Connection path prefix
        // 2. Username and password
        // 3. Timeout
        return builder;
    }

    private BulkProcessor.Builder applyBulkConfig(
            RestHighLevelClient client, EsSinkConfig config, BulkProcessor.Listener listener) {
        BulkProcessor.Builder builder =
                BulkProcessor.builder(
                        new BulkRequestConsumerFactory() {
                            @Override
                            public void accept(
                                    BulkRequest bulkRequest,
                                    ActionListener<BulkResponse> bulkResponseActionListener) {
                                client.bulkAsync(
                                        bulkRequest,
                                        RequestOptions.DEFAULT,
                                        bulkResponseActionListener);
                            }
                        },
                        listener);
        // Possible feature: move these to config
        // execute the bulk every 10 000 requests
        builder.setBulkActions(1000);
        // flush the bulk every 5mb
        builder.setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB));
        // flush the bulk every 5 seconds whatever the number of requests
        builder.setFlushInterval(TimeValue.timeValueSeconds(5));
        // Set the number of concurrent requests
        builder.setConcurrentRequests(1);
        // Set a custom backoff policy which will initially wait for 100ms, increase exponentially
        // and retries up to three times.
        builder.setBackoffPolicy(
                BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3));
        return builder;
    }

    private BulkProcessor createBulkProcessor() {
        BulkProcessor.Builder builder =
                applyBulkConfig(this.client, this.config, new BulkListener());
        return builder.build();
    }

    private class BulkListener implements BulkProcessor.Listener {

        /** This method is called just before bulk is executed. */
        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
            LOG.info("Sending bulk of {} actions to Elasticsearch.", request.numberOfActions());
        }

        /** This method is called after bulk execution. */
        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            LOG.info("Sent bulk of {} actions to Elasticsearch.", request.numberOfActions());
        }

        /** This method is called when the bulk failed and raised a Throwable */
        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            LOG.info(
                    "Bulk of {} actions failed. Failure: {}",
                    request.numberOfActions(),
                    failure.getMessage());
        }
    }

    /**
     * Kind of serialization.
     *
     * @param row
     * @return Map from Field name to Value
     */
    private Map<String, Object> buildDoc(SinkRow row) {
        Map<String, Object> doc = new HashMap();
        for (int i = 0; i < getTableSchema().getNumColumns(); i++) {
            doc.put(getTableSchema().getColumnDesc(i).getName(), row.get(i));
        }
        return doc;
    }

    /**
     * use primary keys as id concatenated by '_'
     *
     * @param row
     * @return
     */
    private String buildId(SinkRow row) {
        String id;
        if (primaryKeyIndexes.isEmpty()) {
            id = row.get(0).toString();
        } else {
            List<String> keys =
                    primaryKeyIndexes.stream()
                            .map(index -> row.get(primaryKeyIndexes.get(index)).toString())
                            .collect(Collectors.toList());
            id = String.join(config.getDelimiter(), keys);
        }
        return id;
    }

    private void processUpsert(SinkRow row) {
        Map<String, Object> doc = buildDoc(row);
        final String key = buildId(row);

        UpdateRequest updateRequest =
                new UpdateRequest(config.getIndex(), "doc", key).doc(doc).upsert(doc);
        bulkProcessor.add(updateRequest);
    }

    private void processDelete(SinkRow row) {
        final String key = buildId(row);
        DeleteRequest deleteRequest = new DeleteRequest(config.getIndex(), "doc", key);
        bulkProcessor.add(deleteRequest);
    }

    private void writeRow(SinkRow row) {
        switch (row.getOp()) {
            case INSERT:
            case UPDATE_INSERT:
                processUpsert(row);
                break;
            case DELETE:
            case UPDATE_DELETE:
                processDelete(row);
                break;
            default:
                throw Status.INVALID_ARGUMENT
                        .withDescription("unspecified row operation")
                        .asRuntimeException();
        }
    }

    @Override
    public void write(Iterator<SinkRow> rows) {
        while (rows.hasNext()) {
            try (SinkRow row = rows.next()) {
                writeRow(row);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void sync() {
        try {
            bulkProcessor.flush();
        } catch (Exception e) {
            throw io.grpc.Status.INTERNAL
                    .withDescription(String.format(ERROR_REPORT_TEMPLATE, e.getMessage()))
                    .asRuntimeException();
        }
    }

    @Override
    public void drop() {
        try {
            bulkProcessor.close();
            client.close();
        } catch (IOException e) {
            throw io.grpc.Status.INTERNAL
                    .withDescription(String.format(ERROR_REPORT_TEMPLATE, e.getMessage()))
                    .asRuntimeException();
        }
    }

    public RestHighLevelClient getClient() {
        return client;
    }
}
