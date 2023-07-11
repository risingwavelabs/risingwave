package com.risingwave.connector.api.sink;

import java.util.Iterator;

public interface SinkWriter {
    void beginEpoch(long epoch);

    void write(Iterator<SinkRow> rows);

    void barrier(boolean isCheckpoint);

    void drop();
}
