package com.risingwave.connector;

import com.risingwave.connector.EsSink.RequestTracker;
import com.risingwave.connector.api.sink.SinkRow;
import java.util.concurrent.TimeUnit;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticBulkProcessorAdapter implements BulkProcessorAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(EsSink.class);
    BulkProcessor esBulkProcessor;

    private class BulkListener implements BulkProcessor.Listener {
        private final RequestTracker requestTracker;

        public BulkListener(RequestTracker requestTracker) {
            this.requestTracker = requestTracker;
        }

        /** This method is called just before bulk is executed. */
        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
            LOG.debug("Sending bulk of {} actions to Elasticsearch.", request.numberOfActions());
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
                LOG.debug("Sent bulk of {} actions to Elasticsearch.", request.numberOfActions());
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

    public ElasticBulkProcessorAdapter(
            RequestTracker requestTracker, ElasticRestHighLevelClientAdapter client) {
        BulkProcessor.Builder builder =
                BulkProcessor.builder(
                        (ElasticBulkRequestConsumerFactory)
                                (bulkRequest, bulkResponseActionListener) ->
                                        client.bulkAsync(
                                                bulkRequest,
                                                RequestOptions.DEFAULT,
                                                bulkResponseActionListener),
                        new BulkListener(requestTracker));
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
        this.esBulkProcessor = builder.build();
    }

    @Override
    public void flush() {
        esBulkProcessor.flush();
    }

    @Override
    public void awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
        esBulkProcessor.awaitClose(timeout, unit);
    }

    @Override
    public void addRow(SinkRow row, String indexName, RequestTracker requestTracker) {
        final String index = (String) row.get(0);
        final String key = (String) row.get(1);
        String doc = (String) row.get(2);

        UpdateRequest updateRequest;
        if (indexName != null) {
            updateRequest = new UpdateRequest(indexName, "_doc", key).doc(doc, XContentType.JSON);
        } else {
            updateRequest = new UpdateRequest(indexName, "_doc", key).doc(doc, XContentType.JSON);
        }
        updateRequest.docAsUpsert(true);
        requestTracker.addWriteTask();
        this.esBulkProcessor.add(updateRequest);
    }

    @Override
    public void deleteRow(SinkRow row, String indexName, RequestTracker requestTracker) {
        final String index = (String) row.get(0);
        final String key = (String) row.get(1);

        DeleteRequest deleteRequest;
        if (indexName != null) {
            deleteRequest = new DeleteRequest(indexName, "_doc", key);
        } else {
            deleteRequest = new DeleteRequest(index, "_doc", key);
        }
        requestTracker.addWriteTask();
        this.esBulkProcessor.add(deleteRequest);
    }
}
