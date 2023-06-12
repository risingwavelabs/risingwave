package com.risingwave.connector;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.Sink;
import com.risingwave.connector.api.sink.SinkBase;
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.proto.Data;
import io.grpc.Status;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.RequestOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;

public class EsSink extends SinkBase {
    private static final String ERROR_REPORT_TEMPLATE = "Error when exec %s, message %s";

    private final EsSinkConfig config;
    private final Connection conn;

    private final BulkProcessor bulkProcessor;
    private final RestHighLevelClient client;
    private static final Logger LOG = LoggerFactory.getLogger(EsSink.class);
    // For bulk listener
    private volatile long lastSendTime = 0;
    private volatile long ackTime = Long.MAX_VALUE;
    public EsSink(EsSinkConfig config, TableSchema tableSchema) {
        super(tableSchema);

        // try and test
        HttpHost host = HttpHost.create(config.getEsUrl());
        this.config = config;
        this.client =
                new RestHighLevelClient(
                        configureRestClientBuilder(
                                RestClient.builder(host),
                                config));
        this.bulkProcessor = createBulkProcessor();
        try {
            this.conn = DriverManager.getConnection(config.getEsUrl());
            this.conn.setAutoCommit(false);
        } catch (SQLException e) {
            throw Status.INTERNAL
                    .withDescription(
                            String.format(ERROR_REPORT_TEMPLATE, e.getSQLState(), e.getMessage()))
                    .asRuntimeException();
        }
    }

    private static RestClientBuilder configureRestClientBuilder(
            RestClientBuilder builder, EsSinkConfig config) {
        // 1. Connection path prefix
        // 2. Username and password
        // 3. Timeout
        return builder;
    }

    private BulkProcessor.Builder applyBulkConfig (
            RestHighLevelClient client,
            EsSinkConfig config,
            BulkProcessor.Listener listener
            ) {
        BulkProcessor.Builder builder =
                BulkProcessor.builder(
                        new BulkRequestConsumerFactory() { // This cannot be inlined as a
                            // lambda because then
                            // deserialization fails
                            @Override
                            public void accept(
                                    BulkRequest bulkRequest,
                                    ActionListener<BulkResponse>
                                            bulkResponseActionListener) {
                                client.bulkAsync(
                                        bulkRequest,
                                        RequestOptions.DEFAULT,
                                        bulkResponseActionListener);
                            }
                        },
                        listener);
        return builder;
    }

    private BulkProcessor createBulkProcessor() {
        BulkProcessor.Builder builder = applyBulkConfig(this.client, this.config, new BulkListener());
        builder.setConcurrentRequests(0);

        return builder.build();
    }

    private class BulkListener implements BulkProcessor.Listener {

        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
            LOG.info("Sending bulk of {} actions to Elasticsearch.", request.numberOfActions());
            lastSendTime = System.currentTimeMillis();
            // numBytesOutCounter.inc(request.estimatedSizeInBytes());
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            ackTime = System.currentTimeMillis();
            /*
            enqueueActionInMailbox(
                    () -> extractFailures(request, response), "elasticsearchSuccessCallback");
            */
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            /*
            enqueueActionInMailbox(
                    () -> {
                        throw new FlinkRuntimeException("Complete bulk has failed.", failure);
                    },
                    "elasticsearchErrorCallback");
            */
        }
    }

    private void writeRow(SinkRow row) {
        switch (row.getOp()) {
            case INSERT:
            case UPDATE_INSERT:

                break;
            case DELETE:
            case UPDATE_DELETE:
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

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void sync() {

    }

    @Override
    public void drop() {
        try {
            conn.close();
        } catch (SQLException e) {
            throw io.grpc.Status.INTERNAL
                    .withDescription(
                            String.format(ERROR_REPORT_TEMPLATE, e.getSQLState(), e.getMessage()))
                    .asRuntimeException();
        }
    }

    public Connection getConn() {
        return conn;
    }
}
