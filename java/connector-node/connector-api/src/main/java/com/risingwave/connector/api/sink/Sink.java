package com.risingwave.connector.api.sink;

import java.util.Iterator;

public interface Sink {
    void write(Iterator<SinkRow> rows);

    void sync();

    void drop();
}
