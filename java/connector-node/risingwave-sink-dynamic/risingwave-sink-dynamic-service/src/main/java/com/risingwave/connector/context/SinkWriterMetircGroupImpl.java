package com.risingwave.connector.context;

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
