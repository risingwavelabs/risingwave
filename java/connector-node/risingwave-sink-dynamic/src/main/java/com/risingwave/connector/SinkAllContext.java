package com.risingwave.connector;

import java.util.OptionalLong;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;

public class SinkAllContext {}

class SinkWriterContext implements org.apache.flink.api.connector.sink.Sink.InitContext {

    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
        return SimpleUserCodeClassLoader.create(Thread.currentThread().getContextClassLoader());
    }

    @Override
    public MailboxExecutor getMailboxExecutor() {
        return null;
    }

    @Override
    public org.apache.flink.api.connector.sink.Sink.ProcessingTimeService
            getProcessingTimeService() {
        return null;
    }

    @Override
    public int getSubtaskId() {
        return (int) (Math.random() * 1000000);
    }

    @Override
    public int getNumberOfParallelSubtasks() {
        return 0;
    }

    @Override
    public SinkWriterMetricGroup metricGroup() {
        return null;
    }

    @Override
    public OptionalLong getRestoredCheckpointId() {
        return OptionalLong.empty();
    }
}

class SinkWriterContextV2 implements Sink.InitContext {

    // Can't use for sink
    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
        return null;
    }

    // Can't use for sink
    @Override
    public MailboxExecutor getMailboxExecutor() {
        return null;
    }

    @Override
    public ProcessingTimeService getProcessingTimeService() {
        return null;
    }

    // Can't use for sink

    @Override
    public int getSubtaskId() {
        return (int) (Math.random() * 1000000);
    }

    // Can't use for sink
    @Override
    public int getNumberOfParallelSubtasks() {
        return 0;
    }

    // Can't use for sink

    // Can't use for sink
    @Override
    public SinkWriterMetricGroup metricGroup() {
        return null;
    }

    @Override
    public OptionalLong getRestoredCheckpointId() {
        return OptionalLong.empty();
    }

    // Can't use for sink
    @Override
    public SerializationSchema.InitializationContext asSerializationSchemaInitializationContext() {
        return null;
    }
}
