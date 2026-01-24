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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.connector.api.sink.SinkWriterBase;
import io.grpc.Status;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpHost;
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
    private static final int INDEX_INDEX = 0;
    private static final int KEY_INDEX = 1;
    private static final int DOC_INDEX = 2;
    private static final int ROUTING_INDEX = 3;

    private final EsSinkConfig config;
    private BulkProcessorAdapter bulkProcessor;

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
        if (config.getConnector().equals("elasticsearch_v1")) {
            ElasticRestHighLevelClientAdapter client =
                    new ElasticRestHighLevelClientAdapter(host, config);
            this.bulkProcessor =
                    new ElasticBulkProcessorAdapter(this.requestTracker, client, config);
        } else if (config.getConnector().equals("opensearch_v1")) {
            OpensearchRestHighLevelClientAdapter client =
                    new OpensearchRestHighLevelClientAdapter(host, config);
            this.bulkProcessor =
                    new OpensearchBulkProcessorAdapter(this.requestTracker, client, config);
        } else {
            throw new RuntimeException("Sink type must be elasticsearch or opensearch");
        }
    }

    private void writeRow(SinkRow row)
            throws JsonMappingException, JsonProcessingException, InterruptedException {
        final String key = (String) row.get(KEY_INDEX);
        String doc = (String) row.get(DOC_INDEX);
        final String index;
        if (config.getIndex() == null) {
            index = (String) row.get(INDEX_INDEX);
        } else {
            index = config.getIndex();
        }
        String routing = null;
        if (config.getRoutingColumn() != null) {
            routing = (String) row.get(ROUTING_INDEX);
        }
        switch (row.getOp()) {
            case INSERT:
            case UPDATE_INSERT:
                this.bulkProcessor.addRow(index, key, doc, routing);
                break;
            case DELETE:
            case UPDATE_DELETE:
                this.bulkProcessor.deleteRow(index, key, routing);
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
        } catch (Exception e) {
            throw io.grpc.Status.INTERNAL
                    .withDescription(String.format(ERROR_REPORT_TEMPLATE, e.getMessage()))
                    .asRuntimeException();
        }
    }
}
