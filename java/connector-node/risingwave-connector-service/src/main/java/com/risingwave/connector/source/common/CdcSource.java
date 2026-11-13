/*
 * Copyright 2026 RisingWave Labs
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

package com.risingwave.connector.source.common;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.source.SourceTypeE;
import java.lang.management.ManagementFactory;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import javax.management.JMException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A CDC source of one connector. {@link CdcSourceFactory} assembles it, and the generic engine,
 * consumer, and validation code ask it for everything that differs between connectors.
 */
public abstract class CdcSource {
    private static final Logger LOG = LoggerFactory.getLogger(CdcSource.class);

    private static final String WAIT_FOR_STREAMING_START_TIMEOUT_SECS =
            "cdc.source.wait.streaming.start.timeout";
    private static final String SNAPSHOT_MODE_KEY = "debezium.snapshot.mode";
    private static final String SNAPSHOT_MODE_BACKFILL = "rw_cdc_backfill";
    private static final String NEEDS_OFFSET_COMMIT_ON_CHECKPOINT_KEY =
            "cdc.source.needs.offset.commit.on.checkpoint";

    private final long sourceId;
    private final String startOffset;
    private final Map<String, String> userProps;
    private final boolean snapshotDone;
    private final boolean isCdcSourceJob;
    private final boolean isBackfillSource;
    private final int waitStreamingStartTimeout;
    private final boolean needsOffsetCommitOnCheckpoint;

    protected CdcSource(
            long sourceId,
            String startOffset,
            Map<String, String> userProps,
            boolean snapshotDone,
            boolean isCdcSourceJob) {
        this.sourceId = sourceId;
        this.startOffset = startOffset;
        this.userProps = userProps;
        this.snapshotDone = snapshotDone;
        this.isCdcSourceJob = isCdcSourceJob;
        this.isBackfillSource = SNAPSHOT_MODE_BACKFILL.equals(userProps.get(SNAPSHOT_MODE_KEY));
        this.waitStreamingStartTimeout =
                Integer.parseInt(
                        userProps.getOrDefault(WAIT_FOR_STREAMING_START_TIMEOUT_SECS, "60"));
        this.needsOffsetCommitOnCheckpoint =
                Boolean.parseBoolean(userProps.get(NEEDS_OFFSET_COMMIT_ON_CHECKPOINT_KEY));
    }

    public abstract SourceTypeE getSourceType();

    /**
     * Whether Debezium may acknowledge a record only after RisingWave has checkpointed it. This is
     * needed when the acknowledged offset is committed back to the upstream database, where it lets
     * the database discard the change log before the offset.
     */
    public boolean needsOffsetCommitOnCheckpoint() {
        return needsOffsetCommitOnCheckpoint;
    }

    /**
     * Checks the WITH options of the connector and then the upstream database through the
     * connector's {@link CdcSourceValidator}, as {@code CREATE SOURCE} and {@code CREATE TABLE} do
     * before accepting the statement.
     */
    public abstract void validate(TableSchema tableSchema, boolean isBackfillTable)
            throws Exception;

    public long getSourceId() {
        return sourceId;
    }

    public String getStartOffset() {
        return startOffset;
    }

    public boolean hasStartOffset() {
        return startOffset != null && !startOffset.isBlank();
    }

    public Map<String, String> getUserProps() {
        return userProps;
    }

    public boolean isSnapshotDone() {
        return snapshotDone;
    }

    public boolean isCdcSourceJob() {
        return isCdcSourceJob;
    }

    public boolean isBackfillSource() {
        return isBackfillSource;
    }

    public int getWaitStreamingStartTimeout() {
        return waitStreamingStartTimeout;
    }

    /**
     * Prepares the upstream database before the Debezium engine starts, such as creating the
     * objects the engine reads from.
     */
    public void prepareUpstream() throws Exception {}

    /** Returns the offset that RisingWave persists for a change event. */
    public Map<String, ?> getEventSourceOffset(SourceRecord record, CdcEventType eventType) {
        return record.sourceOffset();
    }

    /**
     * Waits until Debezium reports the streaming source of {@code dbServerName} as connected.
     * Returns false when the wait is cancelled or times out.
     */
    public boolean waitForStreamingRunning(
            String dbServerName, int waitStreamingStartTimeout, BooleanSupplier shouldContinue)
            throws InterruptedException {
        LOG.info("Waiting for streaming source of {} to start", dbServerName);
        int pollCount = 0;
        while (true) {
            // Check cancellation before readiness: a replacement engine can register metrics
            // under the same server name while the previous reader is being dropped.
            if (!shouldContinue.getAsBoolean()) {
                LOG.info("Cancelled waiting for streaming source of {} to start", dbServerName);
                return false;
            }
            if (isStreamingRunning(dbServerName)) {
                LOG.info("Debezium streaming source of {} started", dbServerName);
                return true;
            }
            if (pollCount > waitStreamingStartTimeout) {
                LOG.error(
                        "Debezium streaming source of {} failed to start in timeout {}",
                        dbServerName,
                        waitStreamingStartTimeout);
                return false;
            }
            TimeUnit.SECONDS.sleep(1); // poll interval; interruption cancels startup
            pollCount++;
        }
    }

    /** Returns the JMX name under which Debezium registers the streaming metrics of the source. */
    protected ObjectName getStreamingMetricsObjectName(String dbServerName)
            throws MalformedObjectNameException {
        return new ObjectName(
                "debezium."
                        + getSourceType().name().toLowerCase(Locale.ROOT)
                        + ":type=connector-metrics,context=streaming,server="
                        + dbServerName);
    }

    // Copy from debezium test suite: io.debezium.embedded.AbstractConnectorTest
    // Notes: although this method is recommended by the community
    // (https://debezium.zulipchat.com/#narrow/stream/302529-community-general/topic/.E2.9C.94.20Embedded.20engine.20has.20started.20StreamingChangeEventSource/near/405121659),
    // but it is not solid enough. As the jmx bean metric is marked as true before starting the
    // binlog client, which may fail to connect the upstream database.
    private boolean isStreamingRunning(String dbServerName) {
        var mbeanServer = ManagementFactory.getPlatformMBeanServer();
        try {
            return (boolean)
                    mbeanServer.getAttribute(
                            getStreamingMetricsObjectName(dbServerName), "Connected");
        } catch (JMException _ex) {
            // ignore the exception, as it is expected when the streaming source
            // (aka. binlog client) is not ready
        }
        return false;
    }
}
