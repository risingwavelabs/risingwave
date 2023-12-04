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

import org.apache.flink.metrics.*;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

/**
 * Error monitoring for sink is a mock implementation of SinkWriterMetricGroup, which just simulates
 * the relevant methods and doesn't do anything about the error
 */
public class SinkWriterMetircGroupImpl extends UnregisteredMetricsGroup
        implements SinkWriterMetricGroup {

    private final Counter numRecordsOutErrors;
    private final Counter numRecordsSendErrors;
    private final Counter numRecordsWritten;
    private final Counter numBytesWritten;
    private final OperatorIOMetricGroup operatorIOMetricGroup;

    public SinkWriterMetircGroupImpl() {
        numRecordsOutErrors = super.counter("numRecordsOutErrors");
        numRecordsSendErrors = super.counter("numRecordsOutErrors");
        numRecordsWritten = super.counter("numRecordsOutErrors");
        numBytesWritten = super.counter("numRecordsOutErrors");
        operatorIOMetricGroup = UnregisteredMetricsGroup.createOperatorIOMetricGroup();
    }

    @Override
    public Counter getNumRecordsOutErrorsCounter() {
        return numRecordsOutErrors;
    }

    @Override
    public Counter getNumRecordsSendErrorsCounter() {
        return numRecordsSendErrors;
    }

    @Override
    public Counter getNumRecordsSendCounter() {
        return numRecordsWritten;
    }

    @Override
    public Counter getNumBytesSendCounter() {
        return numBytesWritten;
    }

    @Override
    public void setCurrentSendTimeGauge(Gauge<Long> currentSendTimeGauge) {
        super.gauge("currentSendTime", currentSendTimeGauge);
    }

    @Override
    public OperatorIOMetricGroup getIOMetricGroup() {
        return operatorIOMetricGroup;
    }
}
