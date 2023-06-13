package com.risingwave.connector;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.Sink;
import com.risingwave.connector.api.sink.SinkBase;
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.proto.Data;
import io.grpc.Status;

import java.io.IOException;
import java.sql.*;
import java.util.*;
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
import org.elasticsearch.action.bulk.BackoffPolicy;
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
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;

import static org.elasticsearch.xcontent.XContentFactory.*;

/**
 * Note:
 * 1. If no primary key is defined on the DDL, the connector can only operate in
 *    append mode for exchanging INSERT only messages with external system.
 * 2. Index like flink? Currently, index is fixed.
 */
public class EsSink extends SinkBase {
    private static final String ERROR_REPORT_TEMPLATE = "Error when exec %s, message %s";

    private final EsSinkConfig config;
    private final BulkProcessor bulkProcessor;
    private final RestHighLevelClient client;
    private static final Logger LOG = LoggerFactory.getLogger(EsSink.class);
    // For bulk listener
    private final List<Integer> PrimaryKeyIndexes;
    private final IndexGenerator indexGenerator;
    public EsSink(EsSinkConfig config, TableSchema tableSchema) {
        super(tableSchema);
        HttpHost host;
        try {
             host = HttpHost.create(config.getEsUrl());
        } catch (IllegalArgumentException e) {
            throw Status.INVALID_ARGUMENT
                    .withDescription(e.getMessage())
                    .asRuntimeException();
        }

        this.config = config;
        this.client =
                new RestHighLevelClient(
                        configureRestClientBuilder(
                                RestClient.builder(host),
                                config));
        this.bulkProcessor = createBulkProcessor();
        PrimaryKeyIndexes = new ArrayList();
        for (String primaryKey: tableSchema.getPrimaryKeys()) {
            PrimaryKeyIndexes.add(tableSchema.getColumnIndex(primaryKey));
        }
        indexGenerator = IndexGeneratorFactory.createIndexGenerator(config.getIndex());
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
        // execute the bulk every 10 000 requests
        builder.setBulkActions(1000);
        // flush the bulk every 5mb
        builder.setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB));
        // flush the bulk every 5 seconds whatever the number of requests
        builder.setFlushInterval(TimeValue.timeValueSeconds(5));
        // Set the number of concurrent requests
        builder.setConcurrentRequests(1);
        // Set a custom backoff policy which will initially wait for 100ms, increase exponentially and retries up to three times.
        builder.setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3));
        return builder;
    }

    private BulkProcessor createBulkProcessor() {
        BulkProcessor.Builder builder = applyBulkConfig(this.client, this.config, new BulkListener());
        return builder.build();
    }

    private class BulkListener implements BulkProcessor.Listener {

        /* This method is called just before bulk is executed. */
        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
            LOG.info("Sending bulk of {} actions to Elasticsearch.", request.numberOfActions());
        }

        /* This method is called after bulk execution. */
        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) { }

        /* This method is called when the bulk failed and raised a Throwable */
        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) { }
    }

    private Map<String, Object> buildDoc(SinkRow row) {
        Map<String, Object> doc = new HashMap<String, Object>();
        for (int i = 0; i < getTableSchema().getNumColumns(); i++) {
            doc.put(getTableSchema().getColumnDesc(i).getName(), row.get(i));
        }
        return doc;
    }

    /**
     * if we don't have primary key, use the first element by default?
     * primary keys are splitted by _, may
     * @param row
     * @return
     */
    private String buildId(SinkRow row) {
        String id;
        if (PrimaryKeyIndexes.isEmpty()) {
            id = row.get(0).toString();
        } else {
            id = row.get(PrimaryKeyIndexes.get(0)).toString();
            for (int i = 1; i < PrimaryKeyIndexes.size(); i++) {
                id.concat("_").concat(row.get(PrimaryKeyIndexes.get(i)).toString());
            }
        }
        return id;
    }

    private void processUpsert(SinkRow row) {
        Map<String, Object> doc = buildDoc(row);
        final String key = buildId(row);

        UpdateRequest updateRequest = new UpdateRequest(indexGenerator.generate(row), "doc", key)
                .doc(doc)
                .upsert(doc);
        bulkProcessor.add(updateRequest);
    }

    private void processDelete(SinkRow row) {
        final String key = buildId(row);
        DeleteRequest deleteRequest = new DeleteRequest(indexGenerator.generate(row), "doc", key);
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

    /**
     * TODO: fine-grained control
     */
    @Override
    public void sync() {
        bulkProcessor.flush();
    }

    @Override
    public void drop() {
        try {
            bulkProcessor.close();
            client.close();
        } catch (IOException e) {
            throw io.grpc.Status.INTERNAL
                    .withDescription(
                            String.format(ERROR_REPORT_TEMPLATE, e.getMessage()))
                    .asRuntimeException();
        }
    }
}
