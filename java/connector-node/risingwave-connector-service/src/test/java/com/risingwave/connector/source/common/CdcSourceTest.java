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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.risingwave.connector.api.source.SourceTypeE;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.ObjectName;
import org.junit.Test;

public class CdcSourceTest {
    private static CdcSource postgresSource() {
        return CdcSourceFactory.createSource(SourceTypeE.POSTGRES, 1, null, Map.of(), false, false);
    }

    @Test(timeout = 5000)
    public void receiverClosureCancelsLongReadinessWait() throws Exception {
        var polls = new AtomicInteger();
        assertFalse(
                postgresSource()
                        .waitForStreamingRunning(
                                "startup_cancellation_test",
                                3600,
                                () -> polls.incrementAndGet() < 2));
        assertTrue(polls.get() >= 2);
    }

    @Test(timeout = 5000)
    public void interruptionCancelsReadinessWait() {
        assertThrows(
                InterruptedException.class,
                () ->
                        postgresSource()
                                .waitForStreamingRunning(
                                        "startup_interruption_test",
                                        3600,
                                        () -> {
                                            Thread.currentThread().interrupt();
                                            return true;
                                        }));
    }

    @Test(timeout = 5000)
    public void cancellationTakesPrecedenceOverReadiness() throws Exception {
        var server = ManagementFactory.getPlatformMBeanServer();
        var name =
                new ObjectName(
                        "debezium.postgres:type=connector-metrics,context=streaming,server=startup_ready_test");
        server.registerMBean(new StreamingMetrics(), name);
        try {
            assertFalse(
                    postgresSource()
                            .waitForStreamingRunning("startup_ready_test", 3600, () -> false));
            assertTrue(
                    postgresSource()
                            .waitForStreamingRunning("startup_ready_test", 3600, () -> true));
        } finally {
            server.unregisterMBean(name);
        }
    }

    @Test(timeout = 5000)
    public void readinessStillTimesOut() throws Exception {
        assertFalse(
                postgresSource().waitForStreamingRunning("startup_timeout_test", 0, () -> true));
    }

    @Test
    public void offsetCommitOnCheckpointFollowsTheReaderProperty() {
        var key = "cdc.source.needs.offset.commit.on.checkpoint";
        assertTrue(
                CdcSourceFactory.createSource(
                                SourceTypeE.POSTGRES, 1, null, Map.of(key, "true"), false, false)
                        .needsOffsetCommitOnCheckpoint());
        assertFalse(
                CdcSourceFactory.createSource(
                                SourceTypeE.POSTGRES, 1, null, Map.of(key, "false"), false, false)
                        .needsOffsetCommitOnCheckpoint());
        assertFalse(postgresSource().needsOffsetCommitOnCheckpoint());
    }

    public interface StreamingMetricsMBean {
        boolean isConnected();
    }

    public static class StreamingMetrics implements StreamingMetricsMBean {
        @Override
        public boolean isConnected() {
            return true;
        }
    }
}
