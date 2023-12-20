package com.risingwave.connector.api.tracing;

import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;

public class TracingSlf4jLoggerFactory implements ILoggerFactory {
    @Override
    public Logger getLogger(String name) {
        return new TracingSlf4jAdapter(name);
    }
}
