/*
 * Copyright 2023 RisingWave Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.risingwave.mock.flink.runtime.context;

import java.util.OptionalLong;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;

/*
 * A simulated implementation of the InitContext, passed down and utilized in the downstream of the sink (SinkV2).
 */
public class SinkWriterContextV2 implements Sink.InitContext {

    /** A character timer. */
    class ProcessingTimeServiceImpl implements ProcessingTimeService {
        private final ScheduledThreadPoolExecutor timerService;

        ProcessingTimeServiceImpl() {
            this.timerService = new ScheduledThreadPoolExecutor(1);
        }

        @Override
        public long getCurrentProcessingTime() {
            return System.currentTimeMillis();
        }

        /**
         * @param timestamp Time when the task is to be executed (in processing time)
         * @param target The task to be executed
         * @return
         */
        @Override
        public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback target) {
            long delay = 0;
            if (timestamp >= getCurrentProcessingTime()) {
                delay = timestamp - getCurrentProcessingTime() + 1;
            }
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

    // The sinkWriter's monitor, which is responsible for monitoring downstream data for errors,
    // currently just mocks one and doesn't do anything with the error
    SinkWriterMetricGroup sinkWriterMetricGroup;
    // A character timer.
    ProcessingTimeService processingTimeService;
    SimpleUserCodeClassLoader simpleUserCodeClassLoader;
    // Downstream executes some tasks serially by registering them with `MailboxExecutor`
    MailboxExecutor mailboxExecutor;

    InitializationContextImpl initializationContext;

    public SinkWriterContextV2() {
        sinkWriterMetricGroup = new SinkWriterMetircGroupImpl();
        processingTimeService = new ProcessingTimeServiceImpl();
        simpleUserCodeClassLoader =
                SimpleUserCodeClassLoader.create(Thread.currentThread().getContextClassLoader());
        mailboxExecutor = new MailBoxExecImpl();
        initializationContext =
                new InitializationContextImpl(sinkWriterMetricGroup, simpleUserCodeClassLoader);
    }

    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
        return simpleUserCodeClassLoader;
    }

    @Override
    public MailboxExecutor getMailboxExecutor() {
        return mailboxExecutor;
    }

    @Override
    public ProcessingTimeService getProcessingTimeService() {
        return processingTimeService;
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
    public int getAttemptNumber() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SinkWriterMetricGroup metricGroup() {
        return sinkWriterMetricGroup;
    }

    @Override
    public OptionalLong getRestoredCheckpointId() {
        return OptionalLong.empty();
    }

    @Override
    public SerializationSchema.InitializationContext asSerializationSchemaInitializationContext() {
        return initializationContext;
    }

    @Override
    public boolean isObjectReuseEnabled() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <IN> TypeSerializer<IN> createInputSerializer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public JobID getJobId() {
        throw new UnsupportedOperationException();
    }

    class InitializationContextImpl implements SerializationSchema.InitializationContext {

        MetricGroup metricGroup;
        UserCodeClassLoader userCodeClassLoader;

        InitializationContextImpl(
                MetricGroup metricGroup, UserCodeClassLoader userCodeClassLoader) {
            this.metricGroup = metricGroup;
            this.userCodeClassLoader = userCodeClassLoader;
        }

        @Override
        public MetricGroup getMetricGroup() {
            return metricGroup;
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return userCodeClassLoader;
        }
    }
}
