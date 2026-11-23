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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.risingwave.connector.source.common.CdcSource;
import com.risingwave.connector.source.common.CdcSourceFactory;
import com.risingwave.connector.source.common.DbzConnectorConfig;
import com.risingwave.java.binding.CdcSourceChannel;
import com.risingwave.proto.ConnectorServiceProto.GetEventStreamRequest;
import com.risingwave.proto.ConnectorServiceProto.GetEventStreamResponse;
import com.risingwave.proto.ConnectorServiceProto.SourceType;
import io.debezium.embedded.async.AsyncEngineConfig;
import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.junit.Test;

public class DbzCdcEngineRunnerTest {
    private static CdcSource source() {
        return CdcSourceFactory.createSource(
                GetEventStreamRequest.newBuilder()
                        .setSourceId(27413)
                        .setSourceType(SourceType.POSTGRES)
                        .putAllProperties(
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
                                        "cdc.source.wait.streaming.start.timeout", "3600"))
                        .setSnapshotDone(true)
                        .build());
    }

    @Test(timeout = 10000)
    public void receiverClosureDuringStartupAllowsEngineCleanup() throws Exception {
        var source = source();
        var config = source.getConfig();
        var engine = new WaitingEngine(source, config);
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
        var source = source();
        var config = source.getConfig();
        var engine = new WaitingEngine(source, config);
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
        var source = source();
        var config = source.getConfig();
        var engine = new WaitingEngine(source, config);
        engine.failStop = true;
        var runner = new DbzCdcEngineRunner(config, engine);
        try {
            assertFalse(runner.start(() -> false));
            assertTrue(engine.started.await(5, TimeUnit.SECONDS));
            assertThrows(IllegalStateException.class, runner::stop);
            assertTrue(engine.terminated.await(5, TimeUnit.SECONDS));
            assertFalse(runner.isRunning());
            assertNotNull(runner.getStopFailure());
            engine.failStop = false;
            runner.stop();
            assertEquals(1, engine.stopCalls.get());
            assertNotNull(runner.getStopFailure());
            runner.stop();
            assertEquals(1, engine.stopCalls.get());
        } finally {
            engine.failStop = false;
            runner.stop();
        }
    }

    @Test(timeout = 10000)
    public void pendingCleanupDoesNotTriggerAnotherStop() throws Exception {
        var source = source();
        var config = source.getConfig();
        var pending = new AtomicBoolean(true);
        var engine =
                new WaitingEngine(source, config) {
                    @Override
                    public boolean hasPendingCleanup() {
                        return pending.get();
                    }
                };
        var runner = new DbzCdcEngineRunner(config, engine);
        try {
            assertFalse(runner.start(() -> false));
            assertTrue(engine.started.await(5, TimeUnit.SECONDS));
            runner.stop();
            assertFalse(runner.isRunning());
            assertNull(runner.getStopFailure());
            assertTrue(runner.hasPendingCleanup());
            runner.stop();
            assertTrue(runner.hasPendingCleanup());
            pending.set(false);
            assertFalse(runner.hasPendingCleanup());
            runner.stop();
            assertEquals(1, engine.stopCalls.get());
        } finally {
            runner.stop();
        }
    }

    @Test(timeout = 10000)
    public void stoppedRunnerCannotBeRestarted() throws Exception {
        var source = source();
        var config = source.getConfig();
        var engine = new DbzCdcEngine(source, config, (ok, message, error) -> {});
        var runner = new DbzCdcEngineRunner(config, engine);
        runner.stop();
        assertFalse(runner.hasPendingCleanup());
        assertThrows(IllegalStateException.class, runner::start);
        assertFalse(runner.isRunning());
    }

    @Test(timeout = 20000)
    public void realEngineCleanupFailureIsReportedWithoutRetry() throws Exception {
        FailStopTask.polling = new CountDownLatch(2);
        FailStopTask.failedStops.set(0);
        FailStopTask.successfulStops.set(0);
        FailStopTask.resourceOpen.set(true);
        var source = source();
        var config = source.getConfig();
        var props = config.getResolvedDebeziumProps();
        props.setProperty("connector.class", FailStopConnector.class.getName());
        props.setProperty("tasks.max", "2");
        props.setProperty(
                "offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore");
        var completed = new CountDownLatch(1);
        var callbackFailure = new AtomicReference<Throwable>();
        var runner =
                DbzCdcEngineRunner.newCdcEngineRunner(
                        source,
                        config,
                        new StreamObserver<GetEventStreamResponse>() {
                            @Override
                            public void onNext(GetEventStreamResponse value) {}

                            @Override
                            public void onError(Throwable failure) {
                                callbackFailure.set(failure);
                                completed.countDown();
                            }

                            @Override
                            public void onCompleted() {
                                completed.countDown();
                            }
                        });
        assertNotNull(runner);
        try {
            assertFalse(runner.start(() -> false));
            assertTrue(FailStopTask.polling.await(5, TimeUnit.SECONDS));
            assertThrows(Exception.class, runner::stop);
            assertTrue(completed.await(5, TimeUnit.SECONDS));
            assertNotNull(callbackFailure.get());
            assertEquals("task cleanup failed", runner.getStopFailure().getMessage());
            assertTrue(FailStopTask.resourceOpen.get());
            assertFalse(runner.isRunning());

            // Repeated stop requests do not attempt task cleanup again.
            runner.stop();
            runner.stop();
            assertNotNull(runner.getStopFailure());
            assertFalse(runner.hasPendingCleanup());
            assertTrue(FailStopTask.resourceOpen.get());
            assertEquals(1, FailStopTask.failedStops.get());
            assertEquals(1, FailStopTask.successfulStops.get());
        } finally {
            runner.stop();
        }
    }

    @Test(timeout = 10000)
    public void realEngineCompletesLateTaskCleanupAfterOneStop() throws Exception {
        LateCreatedTask.entered = new CountDownLatch(1);
        LateCreatedTask.release = new CountDownLatch(1);
        LateCreatedTask.stops.set(0);
        LateCreatedTask.resourceOpen.set(false);
        var source = source();
        var config = source.getConfig();
        var props = config.getResolvedDebeziumProps();
        props.setProperty("connector.class", LateCreatedConnector.class.getName());
        props.setProperty("offset.storage", MemoryOffsetBackingStore.class.getName());
        props.setProperty(AsyncEngineConfig.TASK_MANAGEMENT_TIMEOUT_MS.name(), "100");
        var completed = new CountDownLatch(1);
        var engine =
                new DbzCdcEngine(source, config, (ok, message, error) -> completed.countDown());
        var runner = new DbzCdcEngineRunner(config, engine);
        try {
            assertFalse(runner.start(() -> false));
            assertTrue(LateCreatedTask.entered.await(5, TimeUnit.SECONDS));
            assertThrows(Exception.class, runner::stop);
            assertTrue(runner.hasPendingCleanup());
            LateCreatedTask.release.countDown();
            assertTrue(completed.await(5, TimeUnit.SECONDS));

            assertFalse(runner.isRunning());
            awaitCondition(() -> !runner.hasPendingCleanup());
            assertNull(runner.getStopFailure());
            assertFalse(runner.hasPendingCleanup());
            assertFalse(LateCreatedTask.resourceOpen.get());
            assertEquals(1, LateCreatedTask.stops.get());
            runner.stop();
            assertEquals(1, LateCreatedTask.stops.get());
        } finally {
            LateCreatedTask.release.countDown();
            assertTrue(completed.await(5, TimeUnit.SECONDS));
            engine.stop();
        }
    }

    @Test(timeout = 20000)
    public void jniHandlerRetainsCleanupUntilTaskCreationFinishes() throws Exception {
        LateCreatedTask.entered = new CountDownLatch(1);
        LateCreatedTask.release = new CountDownLatch(1);
        LateCreatedTask.stops.set(0);
        LateCreatedTask.resourceOpen.set(false);
        var source = source();
        var config = source.getConfig();
        var props = config.getResolvedDebeziumProps();
        props.setProperty("connector.class", LateCreatedConnector.class.getName());
        props.setProperty("offset.storage", MemoryOffsetBackingStore.class.getName());
        props.setProperty(AsyncEngineConfig.TASK_MANAGEMENT_TIMEOUT_MS.name(), "100");
        var receiverOpen = new AtomicBoolean(true);
        var channel = mock(CdcSourceChannel.class);
        when(channel.send(nullable(byte[].class))).thenAnswer(invocation -> receiverOpen.get());
        when(channel.sendError(anyString())).thenReturn(false);
        var handler = new JniDbzSourceHandler(source, config, channel);
        JniDbzSourceHandler replacement = null;
        try (var executor = Executors.newSingleThreadExecutor()) {
            var run = executor.submit(handler::start);
            try {
                assertTrue(LateCreatedTask.entered.await(5, TimeUnit.SECONDS));
                receiverOpen.set(false);
                run.get(5, TimeUnit.SECONDS);
                assertNotNull(handler.getStopFailure());
                assertNull(JniDbzSourceRegistry.getSourceHandler(handler.getSourceId()));
                assertTrue(JniDbzSourceRegistry.getPendingCleanupHandlers().contains(handler));
                assertEquals(0, LateCreatedTask.stops.get());
                replacement = new JniDbzSourceHandler(source, config, channel);
                JniDbzSourceRegistry.register(replacement);

                // The registry must own cleanup before any task cleanup record exists.
                LateCreatedTask.release.countDown();
                awaitCondition(() -> LateCreatedTask.stops.get() == 1);
                awaitCondition(
                        () -> !JniDbzSourceRegistry.getPendingCleanupHandlers().contains(handler));
                assertFalse(LateCreatedTask.resourceOpen.get());
                assertNull(handler.getStopFailure());
                assertEquals(
                        replacement, JniDbzSourceRegistry.getSourceHandler(handler.getSourceId()));
            } finally {
                receiverOpen.set(false);
                LateCreatedTask.release.countDown();
                run.get(5, TimeUnit.SECONDS);
                awaitCondition(
                        () -> {
                            handler.stop();
                            return LateCreatedTask.stops.get() == 1;
                        });
                if (replacement != null) {
                    JniDbzSourceRegistry.unregister(replacement);
                }
            }
        }
    }

    public static class LateCreatedConnector extends SlowStopConnector {
        @Override
        public Class<? extends Task> taskClass() {
            return LateCreatedTask.class;
        }
    }

    public static class LateCreatedTask extends SourceTask {
        static CountDownLatch entered;
        static CountDownLatch release;
        private static final AtomicInteger stops = new AtomicInteger();
        private static final AtomicBoolean resourceOpen = new AtomicBoolean();

        public LateCreatedTask() {
            entered.countDown();
            while (true) {
                try {
                    release.await();
                    resourceOpen.set(true);
                    return;
                } catch (InterruptedException ignored) {
                    // Model task creation that continues after the engine is stopped.
                }
            }
        }

        @Override
        public void start(Map<String, String> props) {
            throw new AssertionError("A task created after shutdown must not start");
        }

        @Override
        public List<SourceRecord> poll() {
            return List.of();
        }

        @Override
        public void stop() {
            stops.incrementAndGet();
            resourceOpen.set(false);
        }

        @Override
        public String version() {
            return "1";
        }
    }

    @Test(timeout = 20000)
    public void jniHandlerReleasesCompletedTaskFailure() throws Exception {
        checkJniCleanup(false);
    }

    @Test(timeout = 20000)
    public void jniHandlerReleasesCompletedTaskAndOffsetStoreFailures() throws Exception {
        checkJniCleanup(true);
    }

    public static class ReleasingFailStore extends MemoryOffsetBackingStore {
        private static final AtomicInteger stops = new AtomicInteger();
        private static final AtomicBoolean released = new AtomicBoolean();

        @Override
        public Set<Map<String, Object>> connectorPartitions(String connectorName) {
            return Set.of();
        }

        @Override
        public void stop() {
            super.stop();
            stops.incrementAndGet();
            released.set(true);
            throw new IllegalStateException("offset store stop failed");
        }
    }

    private void checkJniCleanup(boolean failStore) throws Exception {
        FailStopTask.polling = new CountDownLatch(2);
        FailStopTask.failedStops.set(0);
        FailStopTask.successfulStops.set(0);
        FailStopTask.resourceOpen.set(true);
        ReleasingFailStore.stops.set(0);
        ReleasingFailStore.released.set(false);
        var source = source();
        var config = source.getConfig();
        var props = config.getResolvedDebeziumProps();
        props.setProperty("connector.class", FailStopConnector.class.getName());
        props.setProperty("tasks.max", "2");
        props.setProperty(
                "offset.storage",
                failStore
                        ? ReleasingFailStore.class.getName()
                        : MemoryOffsetBackingStore.class.getName());
        var receiverOpen = new AtomicBoolean(true);
        var channel = mock(CdcSourceChannel.class);
        when(channel.send(nullable(byte[].class))).thenAnswer(invocation -> receiverOpen.get());
        when(channel.sendError(anyString())).thenReturn(false);
        var handler = new JniDbzSourceHandler(source, config, channel);
        JniDbzSourceHandler replacement = null;
        try (var executor = Executors.newSingleThreadExecutor()) {
            var run = executor.submit(handler::start);
            assertTrue(FailStopTask.polling.await(5, TimeUnit.SECONDS));
            receiverOpen.set(false);
            run.get(5, TimeUnit.SECONDS);
            assertEquals(1, FailStopTask.failedStops.get());
            assertEquals(1, FailStopTask.successfulStops.get());
            assertNull(JniDbzSourceRegistry.getSourceHandler(handler.getSourceId()));
            assertTrue(FailStopTask.resourceOpen.get());
            assertNotNull(handler.getStopFailure());
            assertFalse(JniDbzSourceRegistry.getPendingCleanupHandlers().contains(handler));
            replacement = new JniDbzSourceHandler(source, config, channel);
            JniDbzSourceRegistry.register(replacement);
            handler.stop();
            assertEquals(1, FailStopTask.failedStops.get());
            assertEquals(replacement, JniDbzSourceRegistry.getSourceHandler(handler.getSourceId()));
            if (failStore) {
                assertTrue(ReleasingFailStore.released.get());
                assertEquals(1, ReleasingFailStore.stops.get());
                assertTrue(
                        java.util.Arrays.stream(handler.getStopFailure().getSuppressed())
                                .anyMatch(
                                        failure ->
                                                "offset store stop failed"
                                                        .equals(failure.getMessage())));
            }
        } finally {
            receiverOpen.set(false);
            handler.stop();
            if (replacement != null) {
                JniDbzSourceRegistry.unregister(replacement);
            }
        }
    }

    @Test(timeout = 20000)
    public void jniHandlerReleasesCleanupThatCompletesAfterTimeout() throws Exception {
        checkSlowJniCleanup(false);
    }

    @Test(timeout = 20000)
    public void jniHandlerReleasesCleanupThatFailsAfterTimeout() throws Exception {
        checkSlowJniCleanup(true);
    }

    private void checkSlowJniCleanup(boolean failStop) throws Exception {
        SlowStopTask.failStop = failStop;
        SlowStopTask.polling = new CountDownLatch(1);
        SlowStopTask.release = new CountDownLatch(1);
        SlowStopTask.cleaned = new CountDownLatch(1);
        SlowStopTask.stops.set(0);
        var source = source();
        var config = source.getConfig();
        var props = config.getResolvedDebeziumProps();
        props.setProperty("connector.class", SlowStopConnector.class.getName());
        props.setProperty(AsyncEngineConfig.TASK_MANAGEMENT_TIMEOUT_MS.name(), "100");
        props.setProperty(
                "offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore");
        var receiverOpen = new AtomicBoolean(true);
        var channel = mock(CdcSourceChannel.class);
        when(channel.send(nullable(byte[].class))).thenAnswer(invocation -> receiverOpen.get());
        when(channel.sendError(anyString())).thenReturn(false);
        var handler = new JniDbzSourceHandler(source, config, channel);
        JniDbzSourceHandler replacement = null;
        try (var executor = Executors.newSingleThreadExecutor()) {
            var run = executor.submit(handler::start);
            try {
                assertTrue(SlowStopTask.polling.await(5, TimeUnit.SECONDS));
                receiverOpen.set(false);
                run.get(5, TimeUnit.SECONDS);
                assertNotNull(handler.getStopFailure());
                assertTrue(JniDbzSourceRegistry.getPendingCleanupHandlers().contains(handler));
                assertEquals(1, SlowStopTask.stops.get());
                replacement = new JniDbzSourceHandler(source, config, channel);
                JniDbzSourceRegistry.register(replacement);

                SlowStopTask.release.countDown();
                assertTrue(SlowStopTask.cleaned.await(5, TimeUnit.SECONDS));
                awaitCondition(
                        () -> !JniDbzSourceRegistry.getPendingCleanupHandlers().contains(handler));
                if (failStop) {
                    assertEquals("late stop failure", handler.getStopFailure().getMessage());
                } else {
                    assertNull(handler.getStopFailure());
                }
                assertEquals(1, SlowStopTask.stops.get());
                assertEquals(
                        replacement, JniDbzSourceRegistry.getSourceHandler(handler.getSourceId()));
            } finally {
                receiverOpen.set(false);
                SlowStopTask.release.countDown();
                run.get(5, TimeUnit.SECONDS);
                handler.stop();
                if (replacement != null) {
                    JniDbzSourceRegistry.unregister(replacement);
                }
            }
        }
    }

    private static void awaitCondition(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertTrue(condition.getAsBoolean());
    }

    public static class SlowStopConnector extends FailStopConnector {
        @Override
        public Class<? extends Task> taskClass() {
            return SlowStopTask.class;
        }

        @Override
        public List<Map<String, String>> taskConfigs(int maxTasks) {
            return List.of(Map.of());
        }
    }

    public static class SlowStopTask extends SourceTask {
        static boolean failStop;
        static CountDownLatch polling;
        static CountDownLatch release;
        static CountDownLatch cleaned;
        private static final AtomicInteger stops = new AtomicInteger();

        @Override
        public void start(Map<String, String> props) {}

        @Override
        public List<SourceRecord> poll() throws InterruptedException {
            polling.countDown();
            Thread.sleep(10);
            return List.of();
        }

        @Override
        public void stop() {
            stops.incrementAndGet();
            while (true) {
                try {
                    release.await();
                    cleaned.countDown();
                    if (failStop) {
                        throw new IllegalStateException("late stop failure");
                    }
                    return;
                } catch (InterruptedException ignored) {
                    // Model cleanup that completes after its caller's bounded wait expires.
                }
            }
        }

        @Override
        public String version() {
            return "1";
        }
    }

    public static class FailStopConnector extends SourceConnector {
        private Map<String, String> props;

        @Override
        public void start(Map<String, String> props) {
            this.props = props;
        }

        @Override
        public Class<? extends Task> taskClass() {
            return FailStopTask.class;
        }

        @Override
        public List<Map<String, String>> taskConfigs(int maxTasks) {
            var first = new HashMap<>(props);
            first.put("fail.stop", "true");
            return List.of(first, props);
        }

        @Override
        public void stop() {}

        @Override
        public ConfigDef config() {
            return new ConfigDef();
        }

        @Override
        public String version() {
            return "1";
        }
    }

    public static class FailStopTask extends SourceTask {
        static CountDownLatch polling;
        private static final AtomicInteger failedStops = new AtomicInteger();
        private static final AtomicInteger successfulStops = new AtomicInteger();
        private static final AtomicBoolean resourceOpen = new AtomicBoolean();
        private boolean failStop;

        @Override
        public void start(Map<String, String> props) {
            failStop = Boolean.parseBoolean(props.get("fail.stop"));
        }

        @Override
        public List<SourceRecord> poll() throws InterruptedException {
            polling.countDown();
            Thread.sleep(10);
            return List.of();
        }

        @Override
        public void stop() {
            if (failStop) {
                failedStops.incrementAndGet();
                throw new IllegalStateException("task cleanup failed");
            } else {
                successfulStops.incrementAndGet();
            }
        }

        @Override
        public String version() {
            return "1";
        }
    }

    // Simulate an engine holding resources before it advertises streaming readiness.
    private static class WaitingEngine extends DbzCdcEngine {
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch terminated = new CountDownLatch(1);
        final CountDownLatch stopped = new CountDownLatch(1);
        final AtomicInteger stopCalls = new AtomicInteger();
        boolean failStop;

        WaitingEngine(CdcSource source, DbzConnectorConfig config) {
            super(source, config, (success, message, error) -> {});
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

        @Override
        public boolean hasPendingCleanup() {
            return stopped.getCount() != 0;
        }
    }
}
