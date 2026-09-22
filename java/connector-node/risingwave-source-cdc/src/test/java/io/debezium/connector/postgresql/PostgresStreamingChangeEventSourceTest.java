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

package io.debezium.connector.postgresql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class PostgresStreamingChangeEventSourceTest {

    @Test
    public void keepAliveMonitorReportsUnexpectedTaskCompletionWhenFailureIsSwallowed() {
        AtomicBoolean stopping = new AtomicBoolean(false);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        ExecutorService executor =
                new PostgresStreamingChangeEventSource.MonitoredKeepAliveExecutor(
                        new SameThreadExecutor(), stopping::get, failure::set);

        Future<?> future =
                executor.submit(
                        () -> {
                            try {
                                throw new RuntimeException("status update failed");
                            } catch (Exception ignored) {
                                return;
                            }
                        });

        assertTrue(future.isDone());
        assertTrue(failure.get() instanceof IllegalStateException);
        assertEquals("Keep-alive thread stopped unexpectedly", failure.get().getMessage());
    }

    @Test
    public void keepAliveMonitorIgnoresTaskCompletionDuringStop() {
        AtomicBoolean stopping = new AtomicBoolean(true);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        ExecutorService executor =
                new PostgresStreamingChangeEventSource.MonitoredKeepAliveExecutor(
                        new SameThreadExecutor(), stopping::get, failure::set);

        Future<?> future = executor.submit(() -> {});

        assertTrue(future.isDone());
        assertNull(failure.get());
    }

    @Test
    public void keepAliveMonitorUnwrapsFutureTaskFailure() {
        AtomicBoolean stopping = new AtomicBoolean(false);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        ExecutorService executor =
                new PostgresStreamingChangeEventSource.MonitoredKeepAliveExecutor(
                        new SameThreadExecutor(), stopping::get, failure::set);
        RuntimeException expected = new RuntimeException("status update failed");

        Future<?> future =
                executor.submit(
                        () -> {
                            throw expected;
                        });

        assertTrue(future.isDone());
        assertSame(expected, failure.get());
    }

    @Test
    public void cleanupSkipsCommitAndSlotDropAfterForcedShutdown() throws SQLException {
        TestJdbcConnection connection = new TestJdbcConnection(false);
        PostgresStreamingChangeEventSource.ConnectionAbortRegistry registry =
                new PostgresStreamingChangeEventSource.ConnectionAbortRegistry();
        AtomicBoolean dropSlot = new AtomicBoolean(true);
        registry.abortAll(Runnable::run);

        PostgresStreamingChangeEventSource.cleanUpConnectionOnStop(
                connection, registry, dropSlot::set, true);

        assertFalse(connection.connected.get());
        assertFalse(connection.committed.get());
        assertFalse(dropSlot.get());
    }

    @Test
    public void cleanupClosesReplicationConnectionWhenCommitFails() {
        TestJdbcConnection connection = new TestJdbcConnection(true);
        PostgresStreamingChangeEventSource.ConnectionAbortRegistry registry =
                new PostgresStreamingChangeEventSource.ConnectionAbortRegistry();
        AtomicBoolean replicationClosed = new AtomicBoolean(false);
        AtomicBoolean dropSlot = new AtomicBoolean(false);

        PostgresStreamingChangeEventSource.cleanUpConnectionOnStop(
                connection,
                registry,
                shouldDropSlot -> {
                    replicationClosed.set(true);
                    dropSlot.set(shouldDropSlot);
                },
                true);

        assertTrue(connection.committed.get());
        assertTrue(replicationClosed.get());
        assertTrue(dropSlot.get());
    }

    @Test
    public void cleanupRechecksForcedShutdownAfterCommit() {
        PostgresStreamingChangeEventSource.ConnectionAbortRegistry registry =
                new PostgresStreamingChangeEventSource.ConnectionAbortRegistry();
        TestJdbcConnection connection =
                new TestJdbcConnection(false, () -> registry.abortAll(Runnable::run));
        AtomicBoolean dropSlot = new AtomicBoolean(true);

        PostgresStreamingChangeEventSource.cleanUpConnectionOnStop(
                connection, registry, dropSlot::set, true);

        assertTrue(connection.committed.get());
        assertTrue(connection.aborted.get());
        assertFalse(dropSlot.get());
    }

    @Test
    public void forcedShutdownAbortsConnectionPublishedDuringStartup() throws SQLException {
        PostgresStreamingChangeEventSource.ConnectionAbortRegistry registry =
                new PostgresStreamingChangeEventSource.ConnectionAbortRegistry();
        AtomicBoolean aborted = new AtomicBoolean(false);

        registry.abortAll(Runnable::run);
        try {
            registry.capture(connection(aborted));
            throw new AssertionError("Expected connection capture to reject forced shutdown");
        } catch (SQLException expected) {
            assertEquals("Connection opened during forced shutdown", expected.getMessage());
        }

        assertTrue(aborted.get());
    }

    @Test
    public void forcedShutdownDoesNotReconnectDuringStartup() throws SQLException {
        PostgresStreamingChangeEventSource.ConnectionAbortRegistry registry =
                new PostgresStreamingChangeEventSource.ConnectionAbortRegistry();

        registry.abortAll(Runnable::run);
        try {
            registry.capture(new TestJdbcConnection(false), true);
            throw new AssertionError("Expected connection capture to reject forced shutdown");
        } catch (SQLException expected) {
            assertEquals("Connection requested during forced shutdown", expected.getMessage());
        }
    }

    @Test
    public void forcedShutdownAbortsEveryTrackedConnection() throws SQLException {
        PostgresStreamingChangeEventSource.ConnectionAbortRegistry registry =
                new PostgresStreamingChangeEventSource.ConnectionAbortRegistry();
        AtomicBoolean firstConnectionAborted = new AtomicBoolean(false);
        AtomicBoolean secondConnectionAborted = new AtomicBoolean(false);

        registry.capture(connection(firstConnectionAborted));
        registry.capture(connection(secondConnectionAborted));
        registry.abortAll(Runnable::run);

        assertTrue(firstConnectionAborted.get());
        assertTrue(secondConnectionAborted.get());
    }

    @Test
    public void trackerCapturesEveryPostgresConnectionUsage() throws SQLException {
        PostgresStreamingChangeEventSource.ConnectionAbortRegistry registry =
                new PostgresStreamingChangeEventSource.ConnectionAbortRegistry();
        List<AtomicBoolean> aborted = new ArrayList<>();
        List<String> connectionUsages =
                Arrays.asList(
                        PostgresConnection.CONNECTION_GENERAL,
                        PostgresConnection.CONNECTION_STREAMING,
                        PostgresConnection.CONNECTION_SLOT_INFO,
                        PostgresConnection.CONNECTION_DROP_SLOT);

        try (PostgresConnection.ConnectionTrackingScope ignored =
                PostgresConnection.trackConnections(registry)) {
            for (String connectionUsage : connectionUsages) {
                AtomicBoolean connectionAborted = new AtomicBoolean(false);
                aborted.add(connectionAborted);
                Connection rawConnection = connection(connectionAborted);
                JdbcConnection jdbcConnection =
                        trackingJdbcConnection(connectionUsage, config -> rawConnection);
                assertSame(rawConnection, jdbcConnection.connection(false));
            }
        }

        registry.abortAll(Runnable::run);
        for (AtomicBoolean connectionAborted : aborted) {
            assertTrue(connectionAborted.get());
        }
    }

    @Test
    public void forcedShutdownRejectsConnectionBeforeOpeningSocket() throws SQLException {
        PostgresStreamingChangeEventSource.ConnectionAbortRegistry registry =
                new PostgresStreamingChangeEventSource.ConnectionAbortRegistry();
        AtomicBoolean connectCalled = new AtomicBoolean(false);
        JdbcConnection jdbcConnection =
                trackingJdbcConnection(
                        PostgresConnection.CONNECTION_SLOT_INFO,
                        config -> {
                            connectCalled.set(true);
                            return connection(new AtomicBoolean(false));
                        });

        registry.abortAll(Runnable::run);

        try (PostgresConnection.ConnectionTrackingScope ignored =
                PostgresConnection.trackConnections(registry)) {
            try {
                jdbcConnection.connection(false);
                throw new AssertionError("Expected a new connection to be rejected");
            } catch (SQLException expected) {
                assertEquals("Connection requested during forced shutdown", expected.getMessage());
            }
        }

        assertFalse(connectCalled.get());
    }

    @Test
    public void attachedTrackerCapturesReconnectFromAnotherThread() throws Exception {
        PostgresStreamingChangeEventSource.ConnectionAbortRegistry registry =
                new PostgresStreamingChangeEventSource.ConnectionAbortRegistry();
        AtomicBoolean firstConnectionAborted = new AtomicBoolean(false);
        AtomicBoolean secondConnectionAborted = new AtomicBoolean(false);
        AtomicInteger connectionCount = new AtomicInteger();
        TestPostgresConnection connection =
                new TestPostgresConnection(
                        config ->
                                connectionCount.getAndIncrement() == 0
                                        ? connection(firstConnectionAborted)
                                        : connection(secondConnectionAborted));
        connection.setConnectionTracker(registry);
        connection.connection(false);
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            executor.submit(
                            () -> {
                                connection.reconnect();
                                return null;
                            })
                    .get(10, TimeUnit.SECONDS);
            registry.abortAll(Runnable::run);
        } finally {
            executor.shutdownNow();
        }

        assertEquals(2, connectionCount.get());
        assertTrue(firstConnectionAborted.get());
        assertTrue(secondConnectionAborted.get());
    }

    @Test
    public void connectionCreationRacingForcedShutdownIsAborted() throws Exception {
        PostgresStreamingChangeEventSource.ConnectionAbortRegistry registry =
                new PostgresStreamingChangeEventSource.ConnectionAbortRegistry();
        AtomicBoolean aborted = new AtomicBoolean(false);
        CountDownLatch connectStarted = new CountDownLatch(1);
        CountDownLatch allowConnectToFinish = new CountDownLatch(1);
        JdbcConnection jdbcConnection =
                trackingJdbcConnection(
                        PostgresConnection.CONNECTION_SLOT_INFO,
                        config -> {
                            connectStarted.countDown();
                            try {
                                if (!allowConnectToFinish.await(10, TimeUnit.SECONDS)) {
                                    throw new SQLException("Timed out waiting to finish connect");
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new SQLException("Interrupted while connecting", e);
                            }
                            return connection(aborted);
                        });
        ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            Future<?> connectionFuture =
                    executor.submit(
                            () -> {
                                try (PostgresConnection.ConnectionTrackingScope ignored =
                                        PostgresConnection.trackConnections(registry)) {
                                    jdbcConnection.connection(false);
                                }
                                return null;
                            });

            assertTrue(connectStarted.await(10, TimeUnit.SECONDS));
            registry.abortAll(Runnable::run);
            allowConnectToFinish.countDown();

            try {
                connectionFuture.get(10, TimeUnit.SECONDS);
                throw new AssertionError("Expected racing connection to be rejected");
            } catch (ExecutionException expected) {
                assertTrue(expected.getCause() instanceof SQLException);
                assertEquals(
                        "Connection opened during forced shutdown",
                        expected.getCause().getMessage());
            }
            assertTrue(aborted.get());
        } finally {
            allowConnectToFinish.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void nestedTrackerRestoresOuterTracker() throws SQLException {
        AtomicBoolean outerTracked = new AtomicBoolean(false);
        AtomicBoolean innerTracked = new AtomicBoolean(false);
        JdbcConnection innerConnection =
                trackingJdbcConnection(
                        PostgresConnection.CONNECTION_STREAMING,
                        config -> connection(new AtomicBoolean(false)));
        JdbcConnection outerConnection =
                trackingJdbcConnection(
                        PostgresConnection.CONNECTION_GENERAL,
                        config -> connection(new AtomicBoolean(false)));

        try (PostgresConnection.ConnectionTrackingScope outerScope =
                PostgresConnection.trackConnections(
                        connection -> {
                            outerTracked.set(true);
                            return connection;
                        })) {
            try (PostgresConnection.ConnectionTrackingScope innerScope =
                    PostgresConnection.trackConnections(
                            connection -> {
                                innerTracked.set(true);
                                return connection;
                            })) {
                innerConnection.connection(false);
            }
            outerConnection.connection(false);
        }

        assertTrue(innerTracked.get());
        assertTrue(outerTracked.get());
    }

    private static JdbcConnection trackingJdbcConnection(
            String connectionUsage, JdbcConnection.ConnectionFactory factory) {
        JdbcConfiguration configuration =
                JdbcConfiguration.adapt(
                        Configuration.empty()
                                .edit()
                                .with("ApplicationName", connectionUsage)
                                .build());
        return new JdbcConnection(
                configuration, TrackingPostgresConnection.trackingFactory(factory), "\"", "\"");
    }

    private static Connection connection(AtomicBoolean aborted) {
        return (Connection)
                Proxy.newProxyInstance(
                        Connection.class.getClassLoader(),
                        new Class<?>[] {Connection.class},
                        (proxy, method, args) -> {
                            switch (method.getName()) {
                                case "abort":
                                    aborted.set(true);
                                    return null;
                                case "isClosed":
                                    return aborted.get();
                                default:
                                    return null;
                            }
                        });
    }

    private static class TestJdbcConnection extends JdbcConnection {
        private final AtomicBoolean connected;
        private final AtomicBoolean committed;
        private final AtomicBoolean aborted;

        TestJdbcConnection(boolean failCommit) {
            this(failCommit, () -> {});
        }

        TestJdbcConnection(boolean failCommit, SqlAction onCommit) {
            this(
                    new AtomicBoolean(false),
                    new AtomicBoolean(false),
                    new AtomicBoolean(false),
                    failCommit,
                    onCommit);
        }

        private TestJdbcConnection(
                AtomicBoolean connected,
                AtomicBoolean committed,
                AtomicBoolean aborted,
                boolean failCommit,
                SqlAction onCommit) {
            super(
                    JdbcConfiguration.empty(),
                    config -> {
                        connected.set(true);
                        return commitConnection(committed, aborted, failCommit, onCommit);
                    },
                    "\"",
                    "\"");
            this.connected = connected;
            this.committed = committed;
            this.aborted = aborted;
        }

        private static Connection commitConnection(
                AtomicBoolean committed,
                AtomicBoolean aborted,
                boolean failCommit,
                SqlAction onCommit) {
            return (Connection)
                    Proxy.newProxyInstance(
                            Connection.class.getClassLoader(),
                            new Class<?>[] {Connection.class},
                            (proxy, method, args) -> {
                                switch (method.getName()) {
                                    case "isClosed":
                                        return aborted.get();
                                    case "getAutoCommit":
                                        return false;
                                    case "abort":
                                        aborted.set(true);
                                        return null;
                                    case "commit":
                                        committed.set(true);
                                        onCommit.run();
                                        if (failCommit) {
                                            throw new SQLException("commit failed");
                                        }
                                        return null;
                                    default:
                                        return null;
                                }
                            });
        }
    }

    @FunctionalInterface
    private interface SqlAction {
        void run() throws SQLException;
    }

    private static class TrackingPostgresConnection extends PostgresConnection {
        private TrackingPostgresConnection() {
            super(JdbcConfiguration.empty(), CONNECTION_GENERAL);
        }

        private static JdbcConnection.ConnectionFactory trackingFactory(
                JdbcConnection.ConnectionFactory delegate) {
            return PostgresConnection.trackCreatedConnections(delegate);
        }
    }

    private static class TestPostgresConnection extends PostgresConnection {
        TestPostgresConnection(JdbcConnection.ConnectionFactory connectionFactory) {
            super(
                    JdbcConfiguration.empty(),
                    PostgresConnection.CONNECTION_GENERAL,
                    connectionFactory);
        }
    }

    private static class SameThreadExecutor extends AbstractExecutorService {
        private boolean shutdown;

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown = true;
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return shutdown;
        }

        @Override
        public void execute(Runnable command) {
            command.run();
        }
    }
}
