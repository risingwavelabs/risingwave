package com.risingwave.connector.api.tracing;

import com.risingwave.java.binding.Binding;

public class TracingSlf4jImpl {
    public static final int ERROR = 0;
    public static final int WARN = 1;
    public static final int INFO = 2;
    public static final int DEBUG = 3;
    public static final int TRACE = 4;

    public static void event(String name, int level, String message) {
        Binding.tracingSlf4jEvent(name, level, message);
    }
}
