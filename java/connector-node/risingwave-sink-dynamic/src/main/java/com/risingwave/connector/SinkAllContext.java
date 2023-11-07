package com.risingwave.connector;

import java.util.OptionalLong;
import java.util.concurrent.*;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.groups.GenericMetricGroup;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeServiceUtil;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor;
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
    class ProcessingTimeServiceImpl implements ProcessingTimeService {
        private final ScheduledThreadPoolExecutor timerService;

        ProcessingTimeServiceImpl() {
            this.timerService = new ScheduledThreadPoolExecutor(1);
        }

        @Override
        public long getCurrentProcessingTime() {
            return System.currentTimeMillis();
        }

        @Override
        public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback target) {
            long delay =
                    ProcessingTimeServiceUtil.getProcessingTimeDelay(
                            timestamp, getCurrentProcessingTime());
            return timerService.schedule(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                target.onProcessingTime(timestamp);
                            } catch (Exception e) {
                                throw new RuntimeException("Callback error" + e.getMessage());
                            }
                        }
                    },
                    delay,
                    TimeUnit.MILLISECONDS);
        }
    }

    SinkWriterMetricGroup sinkWriterMetricGroup;
    ProcessingTimeService processingTimeService;

    MailboxExecutor mailboxExecutor;

    public SinkWriterContextV2() {
        sinkWriterMetricGroup =
                InternalSinkWriterMetricGroup.mock(
                        new GenericMetricGroup(new NoOpMetricRegistry(), null, "rootMetricGroup"));
        processingTimeService = new ProcessingTimeServiceImpl();
        mailboxExecutor = new MailboxProcessor().getMainMailboxExecutor();
    }

    // Can't use for sink
    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
        return null;
    }

    // Can't use for sink
    @Override
    public MailboxExecutor getMailboxExecutor() {
        return mailboxExecutor;
    }

    @Override
    public ProcessingTimeService getProcessingTimeService() {
        return processingTimeService;
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
    @Override
    public SinkWriterMetricGroup metricGroup() {
        return sinkWriterMetricGroup;
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
