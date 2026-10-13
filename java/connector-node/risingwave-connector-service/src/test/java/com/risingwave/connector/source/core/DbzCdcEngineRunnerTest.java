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

package com.risingwave.connector.source.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.connector.source.common.DbzConnectorConfig;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class DbzCdcEngineRunnerTest {
    private static DbzConnectorConfig config() {
        return new DbzConnectorConfig(
                SourceTypeE.POSTGRES,
                27413,
                null,
                Map.of(
                        "hostname", "localhost",
                        "port", "5432",
                        "database.name", "test",
                        "schema.name", "public",
                        "table.name", "test",
                        "slot.name", "startup_test",
                        "username", "postgres",
                        "password", "postgres",
                        "debezium.snapshot.mode", "rw_cdc_backfill",
                        "cdc.source.wait.streaming.start.timeout", "3600"),
                true,
                false);
    }

    @Test(timeout = 10000)
    public void receiverClosureDuringStartupAllowsEngineCleanup() throws Exception {
        var config = config();
        var engine = new WaitingEngine(config);
        var runner = new DbzCdcEngineRunner(config, engine);
        var receiverOpen = new AtomicBoolean(true);
        var polling = new CountDownLatch(1);
        try (var executor = Executors.newSingleThreadExecutor()) {
            var startup =
                    executor.submit(
                            () -> {
                                try {
                                    return runner.start(
                                            () -> {
                                                polling.countDown();
                                                return receiverOpen.get();
                                            });
                                } finally {
                                    // The JNI handler owns cleanup when its startup wait exits.
                                    runner.stop();
                                }
                            });
            try {
                assertTrue(engine.started.await(5, TimeUnit.SECONDS));
                assertTrue(polling.await(5, TimeUnit.SECONDS));
                assertTrue(runner.isRunning());
                receiverOpen.set(false);
                assertFalse(startup.get(5, TimeUnit.SECONDS));
                assertTrue(engine.terminated.await(5, TimeUnit.SECONDS));
                assertFalse(runner.isRunning());
                runner.stop();
                assertEquals(1, engine.stopCalls.get());
            } finally {
                receiverOpen.set(false);
                startup.cancel(true);
                runner.stop();
            }
        }
    }

    @Test(timeout = 10000)
    public void stopDuringStartupDoesNotRestoreRunningState() throws Exception {
        var config = config();
        var engine = new WaitingEngine(config);
        var runner = new DbzCdcEngineRunner(config, engine);
        try (var executor = Executors.newSingleThreadExecutor()) {
            var startup = executor.submit(() -> runner.start());
            try {
                assertTrue(engine.started.await(5, TimeUnit.SECONDS));
                runner.stop();
                assertFalse(startup.get(5, TimeUnit.SECONDS));
                assertTrue(engine.terminated.await(5, TimeUnit.SECONDS));
                assertFalse(runner.isRunning());
                assertEquals(1, engine.stopCalls.get());
            } finally {
                startup.cancel(true);
                runner.stop();
            }
        }
    }

    @Test(timeout = 10000)
    public void stopFailureStillInterruptsEngineThread() throws Exception {
        var config = config();
        var engine = new WaitingEngine(config);
        engine.failStop = true;
        var runner = new DbzCdcEngineRunner(config, engine);
        try {
            assertFalse(runner.start(() -> false));
            assertTrue(engine.started.await(5, TimeUnit.SECONDS));
            assertThrows(IllegalStateException.class, runner::stop);
            assertTrue(engine.terminated.await(5, TimeUnit.SECONDS));
            assertFalse(runner.isRunning());
        } finally {
            runner.stop();
        }
    }

    // Simulate an engine holding resources before it advertises streaming readiness.
    private static class WaitingEngine extends DbzCdcEngine {
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch terminated = new CountDownLatch(1);
        final CountDownLatch stopped = new CountDownLatch(1);
        final AtomicInteger stopCalls = new AtomicInteger();
        boolean failStop;

        WaitingEngine(DbzConnectorConfig config) {
            super(
                    config.getSourceType(),
                    config.getSourceId(),
                    config.getResolvedDebeziumProps(),
                    (success, message, error) -> {});
        }

        @Override
        public void run() {
            started.countDown();
            try {
                stopped.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                terminated.countDown();
            }
        }

        @Override
        public void stop() {
            stopCalls.incrementAndGet();
            if (failStop) {
                throw new IllegalStateException("close failed");
            }
            stopped.countDown();
        }
    }
}
