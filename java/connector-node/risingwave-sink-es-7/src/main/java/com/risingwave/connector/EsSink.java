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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.connector.api.sink.SinkWriterBase;
import io.grpc.Status;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
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
import org.elasticsearch.client.RestHighLevelClientBuilder;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentType;
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
public class EsSink extends SinkWriterBase {
    private static final Logger LOG = LoggerFactory.getLogger(EsSink.class);
    private static final String ERROR_REPORT_TEMPLATE = "Error message %s";

    private final EsSinkConfig config;
    private BulkProcessor bulkProcessor;
    private final RestHighLevelClient client;

    // Used to handle the return message of ES and throw errors
    private final RequestTracker requestTracker;

    class RequestTracker {
        // Used to save the return results of es asynchronous writes. The capacity is Integer.Max
        private final BlockingQueue<EsWriteResultResp> blockingQueue = new LinkedBlockingQueue<>();

        // Count of write tasks in progress
        private int taskCount = 0;

        void addErrResult(String errorMsg) {
            blockingQueue.add(new EsWriteResultResp(errorMsg));
        }

        void addOkResult(int numberOfActions) {
            blockingQueue.add(new EsWriteResultResp(numberOfActions));
        }

        void addWriteTask() {
            taskCount++;
            EsWriteResultResp esWriteResultResp;
            while (true) {
                if ((esWriteResultResp = this.blockingQueue.poll()) != null) {
                    checkEsWriteResultResp(esWriteResultResp);
                } else {
                    return;
                }
            }
        }

        void waitAllFlush() throws InterruptedException {
            while (this.taskCount > 0) {
                EsWriteResultResp esWriteResultResp = this.blockingQueue.poll(10, TimeUnit.SECONDS);
                if (esWriteResultResp == null) {
                    LOG.warn("EsWriteResultResp is null, try wait again");
                } else {
                    checkEsWriteResultResp(esWriteResultResp);
                }
            }
        }

        void checkEsWriteResultResp(EsWriteResultResp esWriteResultResp) {
            if (esWriteResultResp.isOk()) {
                this.taskCount -= esWriteResultResp.getNumberOfActions();
            } else {
                throw new RuntimeException(
                        String.format("Es writer error: %s", esWriteResultResp.getErrorMsg()));
            }
            if (this.taskCount < 0) {
                throw new RuntimeException("The num of task < 0, but blockingQueue is not empty");
            }
        }
    }

    class EsWriteResultResp {

        private boolean isOK;

        private String errorMsg;

        // Number of actions included in completed tasks
        private Integer numberOfActions;

        public boolean isOk() {
            return isOK;
        }

        public EsWriteResultResp(int numberOfActions) {
            this.isOK = true;
            this.numberOfActions = numberOfActions;
        }

        public EsWriteResultResp(String errorMsg) {
            this.isOK = false;
            this.errorMsg = errorMsg;
        }

        public String getErrorMsg() {
            return errorMsg;
        }

        public int getNumberOfActions() {
            return numberOfActions;
        }
    }

    public EsSink(EsSinkConfig config, TableSchema tableSchema) {
        super(tableSchema);
        HttpHost host;
        try {
            host = HttpHost.create(config.getUrl());
        } catch (IllegalArgumentException e) {
            throw Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException();
        }

        this.config = config;
        this.requestTracker = new RequestTracker();

        // ApiCompatibilityMode is enabled to ensure the client can talk to newer version es sever.
        this.client =
                new RestHighLevelClientBuilder(
                                configureRestClientBuilder(RestClient.builder(host), config)
                                        .build())
                        .setApiCompatibilityMode(true)
                        .build();
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
        this.bulkProcessor = createBulkProcessor(this.requestTracker);
    }

    private static RestClientBuilder configureRestClientBuilder(
            RestClientBuilder builder, EsSinkConfig config) {
        // Possible config:
        // 1. Connection path prefix
        // 2. Username and password
        if (config.getPassword() != null && config.getUsername() != null) {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(
                    AuthScope.ANY,
                    new UsernamePasswordCredentials(config.getUsername(), config.getPassword()));
            builder.setHttpClientConfigCallback(
                    httpClientBuilder ->
                            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }
        // 3. Timeout
        return builder;
    }

    private BulkProcessor.Builder applyBulkConfig(
            RestHighLevelClient client, EsSinkConfig config, BulkProcessor.Listener listener) {
        BulkProcessor.Builder builder =
                BulkProcessor.builder(
                        (BulkRequestConsumerFactory)
                                (bulkRequest, bulkResponseActionListener) ->
                                        client.bulkAsync(
                                                bulkRequest,
                                                RequestOptions.DEFAULT,
                                                bulkResponseActionListener),
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

    private BulkProcessor createBulkProcessor(RequestTracker requestTracker) {
        BulkProcessor.Builder builder =
                applyBulkConfig(this.client, this.config, new BulkListener(requestTracker));
        return builder.build();
    }

    private class BulkListener implements BulkProcessor.Listener {
        private final RequestTracker requestTracker;

        public BulkListener(RequestTracker requestTracker) {
            this.requestTracker = requestTracker;
        }

        /** This method is called just before bulk is executed. */
        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
            LOG.info("Sending bulk of {} actions to Elasticsearch.", request.numberOfActions());
        }

        /** This method is called after bulk execution. */
        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            if (response.hasFailures()) {
                String errMessage =
                        String.format(
                                "Bulk of %d actions failed. Failure: %s",
                                request.numberOfActions(), response.buildFailureMessage());
                this.requestTracker.addErrResult(errMessage);
            } else {
                this.requestTracker.addOkResult(request.numberOfActions());
                LOG.info("Sent bulk of {} actions to Elasticsearch.", request.numberOfActions());
            }
        }

        /** This method is called when the bulk failed and raised a Throwable */
        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            String errMessage =
                    String.format(
                            "Bulk of %d actions failed. Failure: %s",
                            request.numberOfActions(), failure.getMessage());
            this.requestTracker.addErrResult(errMessage);
        }
    }

    private void processUpsert(SinkRow row) throws JsonMappingException, JsonProcessingException {
        final String key = (String) row.get(0);
        String doc = (String) row.get(1);

        UpdateRequest updateRequest =
                new UpdateRequest(config.getIndex(), "_doc", key).doc(doc, XContentType.JSON);
        updateRequest.docAsUpsert(true);
        this.requestTracker.addWriteTask();
        bulkProcessor.add(updateRequest);
    }

    private void processDelete(SinkRow row) throws JsonMappingException, JsonProcessingException {
        final String key = (String) row.get(0);

        DeleteRequest deleteRequest = new DeleteRequest(config.getIndex(), "_doc", key);
        this.requestTracker.addWriteTask();
        bulkProcessor.add(deleteRequest);
    }

    private void writeRow(SinkRow row) throws JsonMappingException, JsonProcessingException {
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
            SinkRow row = rows.next();
            try {
                writeRow(row);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override
    public void sync() {
        try {
            this.bulkProcessor.flush();
            this.requestTracker.waitAllFlush();
        } catch (Exception e) {
            throw io.grpc.Status.INTERNAL
                    .withDescription(String.format(ERROR_REPORT_TEMPLATE, e.getMessage()))
                    .asRuntimeException();
        }
    }

    @Override
    public void drop() {
        try {
            bulkProcessor.awaitClose(100, TimeUnit.SECONDS);
            client.close();
        } catch (Exception e) {
            throw io.grpc.Status.INTERNAL
                    .withDescription(String.format(ERROR_REPORT_TEMPLATE, e.getMessage()))
                    .asRuntimeException();
        }
    }

    public RestHighLevelClient getClient() {
        return client;
    }

    private final SimpleDateFormat createSimpleDateFormat(String pattern, TimeZone timeZone) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        sdf.setTimeZone(timeZone);
        return sdf;
    }
}
