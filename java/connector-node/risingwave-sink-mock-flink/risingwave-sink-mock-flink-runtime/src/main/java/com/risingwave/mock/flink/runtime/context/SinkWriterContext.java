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

import java.io.IOException;
import java.util.OptionalLong;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

/*
 * A simulated implementation of the InitContext, passed down and utilized in the downstream of the sink (SinkV1).
 * Method is implemented in `SinkWriterContextV2`.
 */
public class SinkWriterContext implements org.apache.flink.api.connector.sink.Sink.InitContext {
    static class ProcessingTimeCallbackAdapter
            implements ProcessingTimeService.ProcessingTimeCallback {

        Sink.ProcessingTimeService.ProcessingTimeCallback processingTimerCallback;

        public ProcessingTimeCallbackAdapter(
                Sink.ProcessingTimeService.ProcessingTimeCallback processingTimerCallback) {
            this.processingTimerCallback = processingTimerCallback;
        }

        @Override
        public void onProcessingTime(long time) throws IOException, InterruptedException {
            processingTimerCallback.onProcessingTime(time);
        }
    }

    /**
     * Timing task wrapper, responsible for converting `ProcessingTimeService` in sink v2 to
     * `ProcessingTimeService` in sink v1
     */
    static class ProcessingTimeServiceAdapter implements Sink.ProcessingTimeService {

        org.apache.flink.api.common.operators.ProcessingTimeService processingTimeService;

        public ProcessingTimeServiceAdapter(
                org.apache.flink.api.common.operators.ProcessingTimeService processingTimeService) {
            this.processingTimeService = processingTimeService;
        }

        @Override
        public long getCurrentProcessingTime() {
            return processingTimeService.getCurrentProcessingTime();
        }

        @Override
        public void registerProcessingTimer(
                long time, ProcessingTimeCallback processingTimerCallback) {
            processingTimeService.registerTimer(
                    time, new ProcessingTimeCallbackAdapter(processingTimerCallback));
        }
    }

    SinkWriterContextV2 sinkWriterContextV2;

    public SinkWriterContext() {
        sinkWriterContextV2 = new SinkWriterContextV2();
    }

    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
        return sinkWriterContextV2.getUserCodeClassLoader();
    }

    @Override
    public MailboxExecutor getMailboxExecutor() {
        return sinkWriterContextV2.getMailboxExecutor();
    }

    @Override
    public org.apache.flink.api.connector.sink.Sink.ProcessingTimeService
            getProcessingTimeService() {
        return new ProcessingTimeServiceAdapter(sinkWriterContextV2.getProcessingTimeService());
    }

    @Override
    public int getSubtaskId() {
        return sinkWriterContextV2.getSubtaskId();
    }

    @Override
    public int getNumberOfParallelSubtasks() {
        return sinkWriterContextV2.getNumberOfParallelSubtasks();
    }

    @Override
    public SinkWriterMetricGroup metricGroup() {
        return sinkWriterContextV2.metricGroup();
    }

    @Override
    public OptionalLong getRestoredCheckpointId() {
        return sinkWriterContextV2.getRestoredCheckpointId();
    }
}
