package com.risingwave.connector;

import com.risingwave.connector.EsSink.RequestTracker;
import com.risingwave.connector.api.sink.SinkRow;
import java.util.concurrent.TimeUnit;

public interface BulkProcessorAdapter {
    public void addRow(SinkRow row, String indexName, RequestTracker requestTracker);

    public void deleteRow(SinkRow row, String indexName, RequestTracker requestTracker);

    public void flush();

    public void awaitClose(long timeout, TimeUnit unit) throws InterruptedException;
}
