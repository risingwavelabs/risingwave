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

package com.risingwave.connector.context;

import java.util.OptionalLong;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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

public class SinkWriterContextV2 implements Sink.InitContext {
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
    SimpleUserCodeClassLoader simpleUserCodeClassLoader;

    public SinkWriterContextV2() {
        sinkWriterMetricGroup =
                InternalSinkWriterMetricGroup.mock(
                        new GenericMetricGroup(new NoOpMetricRegistry(), null, "rootMetricGroup"));
        processingTimeService = new ProcessingTimeServiceImpl();
        mailboxExecutor = new MailboxProcessor().getMainMailboxExecutor();
        simpleUserCodeClassLoader =
                SimpleUserCodeClassLoader.create(Thread.currentThread().getContextClassLoader());
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
    public SinkWriterMetricGroup metricGroup() {
        return sinkWriterMetricGroup;
    }

    @Override
    public OptionalLong getRestoredCheckpointId() {
        return OptionalLong.empty();
    }

    @Override
    public SerializationSchema.InitializationContext asSerializationSchemaInitializationContext() {
        return null;
    }
}
