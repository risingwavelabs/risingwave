package com.risingwave.metrics;

import com.risingwave.connector.api.sink.SinkRow;
import java.util.Iterator;

public class MonitoredRowIterator implements Iterator<SinkRow> {
    private final Iterator<SinkRow> inner;

    public MonitoredRowIterator(Iterator<SinkRow> inner) {
        this.inner = inner;
    }

    @Override
    public boolean hasNext() {
        return inner.hasNext();
    }

    @Override
    public SinkRow next() {
        ConnectorNodeMetrics.incSinkRowsReceived();
        return inner.next();
    }
}
