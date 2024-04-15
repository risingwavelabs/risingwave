package com.risingwave.connector;

import java.util.concurrent.TimeUnit;

public interface BulkProcessorAdapter {
    public void add(Object request);

    public void flush();

    public void awaitClose(long timeout, TimeUnit unit) throws InterruptedException;
}
