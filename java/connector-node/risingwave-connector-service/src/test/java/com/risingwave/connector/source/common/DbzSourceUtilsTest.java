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
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.ObjectName;
import org.junit.Test;

public class DbzSourceUtilsTest {
    @Test(timeout = 5000)
    public void receiverClosureCancelsLongReadinessWait() throws Exception {
        var polls = new AtomicInteger();
        assertFalse(
                DbzSourceUtils.waitForStreamingRunning(
                        SourceTypeE.POSTGRES,
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
                        DbzSourceUtils.waitForStreamingRunning(
                                SourceTypeE.POSTGRES,
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
                    DbzSourceUtils.waitForStreamingRunning(
                            SourceTypeE.POSTGRES, "startup_ready_test", 3600, () -> false));
            assertTrue(
                    DbzSourceUtils.waitForStreamingRunning(
                            SourceTypeE.POSTGRES, "startup_ready_test", 3600, () -> true));
        } finally {
            server.unregisterMBean(name);
        }
    }

    @Test(timeout = 5000)
    public void readinessStillTimesOut() throws Exception {
        assertFalse(
                DbzSourceUtils.waitForStreamingRunning(
                        SourceTypeE.POSTGRES, "startup_timeout_test", 0, () -> true));
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
