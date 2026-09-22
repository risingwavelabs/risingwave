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
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.connect.errors.ConnectException;
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
    public void cleanupSkipsCommitAfterForcedShutdown() {
        TestJdbcConnection connection = new TestJdbcConnection(false);
        AtomicBoolean replicationClosed = new AtomicBoolean(false);

        PostgresStreamingChangeEventSource.cleanUpConnectionOnStop(
                connection,
                new PostgresStreamingChangeEventSource.AbortableConnection(),
                replicationConnection(replicationClosed),
                false);

        assertFalse(connection.committed.get());
        assertTrue(replicationClosed.get());
    }

    @Test
    public void cleanupClosesReplicationConnectionWhenCommitFails() {
        TestJdbcConnection connection = new TestJdbcConnection(true);
        AtomicBoolean replicationClosed = new AtomicBoolean(false);

        PostgresStreamingChangeEventSource.cleanUpConnectionOnStop(
                connection,
                new PostgresStreamingChangeEventSource.AbortableConnection(),
                replicationConnection(replicationClosed),
                true);

        assertTrue(connection.committed.get());
        assertTrue(replicationClosed.get());
    }

    @Test
    public void cleanupDoesNotReconnectAfterForcedShutdown() throws SQLException {
        TestJdbcConnection connection = new TestJdbcConnection(false);
        PostgresStreamingChangeEventSource.AbortableConnection abortableConnection =
                new PostgresStreamingChangeEventSource.AbortableConnection();
        AtomicBoolean replicationClosed = new AtomicBoolean(false);
        abortableConnection.abort(Runnable::run);

        PostgresStreamingChangeEventSource.cleanUpConnectionOnStop(
                connection, abortableConnection, replicationConnection(replicationClosed), true);

        assertFalse(connection.connected.get());
        assertFalse(connection.committed.get());
        assertTrue(replicationClosed.get());
    }

    @Test
    public void forcedShutdownAbortsConnectionPublishedDuringStartup() throws SQLException {
        PostgresStreamingChangeEventSource.AbortableConnection connection =
                new PostgresStreamingChangeEventSource.AbortableConnection();
        AtomicBoolean aborted = new AtomicBoolean(false);

        connection.abort(Runnable::run);
        try {
            connection.capture(connection(aborted));
            throw new AssertionError("Expected connection capture to reject forced shutdown");
        } catch (SQLException expected) {
            assertEquals("Connection opened during forced shutdown", expected.getMessage());
        }

        assertTrue(aborted.get());
    }

    @Test
    public void forcedShutdownDoesNotReconnectDuringStartup() throws SQLException {
        PostgresStreamingChangeEventSource.AbortableConnection connection =
                new PostgresStreamingChangeEventSource.AbortableConnection();

        connection.abort(Runnable::run);
        try {
            connection.capture(new TestJdbcConnection(false), true);
            throw new AssertionError("Expected connection capture to reject forced shutdown");
        } catch (SQLException expected) {
            assertEquals("Connection requested during forced shutdown", expected.getMessage());
        }
    }

    @Test
    public void forcedShutdownAbortsLatestConnectionAfterReconnect() throws SQLException {
        PostgresStreamingChangeEventSource.AbortableConnection connection =
                new PostgresStreamingChangeEventSource.AbortableConnection();
        AtomicBoolean oldConnectionAborted = new AtomicBoolean(false);
        AtomicBoolean currentConnectionAborted = new AtomicBoolean(false);

        connection.capture(connection(oldConnectionAborted));
        connection.capture(connection(currentConnectionAborted));
        connection.abort(Runnable::run);

        assertFalse(oldConnectionAborted.get());
        assertTrue(currentConnectionAborted.get());
    }

    @Test
    public void forcedShutdownAbortsConnectionCreatedDuringStreamingStartup() throws SQLException {
        PostgresStreamingChangeEventSource.AbortableConnection abortableConnection =
                new PostgresStreamingChangeEventSource.AbortableConnection();
        AtomicBoolean oldConnectionAborted = new AtomicBoolean(false);
        AtomicBoolean newConnectionAborted = new AtomicBoolean(false);
        Connection oldConnection = connection(oldConnectionAborted);
        Connection newConnection = connection(newConnectionAborted);
        AtomicInteger connectionCount = new AtomicInteger();
        JdbcConfiguration configuration =
                JdbcConfiguration.adapt(
                        Configuration.empty()
                                .edit()
                                .with("ApplicationName", PostgresConnection.CONNECTION_STREAMING)
                                .build());
        JdbcConnection jdbcConnection =
                new JdbcConnection(
                        configuration,
                        TrackingPostgresConnection.trackingFactory(
                                config ->
                                        connectionCount.getAndIncrement() == 0
                                                ? oldConnection
                                                : newConnection),
                        "\"",
                        "\"");

        try (PostgresConnection.ConnectionTrackingScope ignored =
                PostgresConnection.trackConnections(
                        PostgresConnection.CONNECTION_STREAMING, abortableConnection::capture)) {
            assertSame(oldConnection, jdbcConnection.connection(false));
            abortableConnection.abort(Runnable::run);
            try {
                jdbcConnection.connection(false);
                throw new AssertionError("Expected a replacement connection to be rejected");
            } catch (SQLException expected) {
                assertEquals("Connection opened during forced shutdown", expected.getMessage());
            }
        }

        assertEquals(2, connectionCount.get());
        assertTrue(oldConnectionAborted.get());
        assertTrue(newConnectionAborted.get());
    }

    @Test
    public void forcedShutdownAbortsGeneralConnectionCreatedDuringProbe() throws SQLException {
        PostgresStreamingChangeEventSource.AbortableConnection abortableConnection =
                new PostgresStreamingChangeEventSource.AbortableConnection();
        AtomicBoolean oldConnectionAborted = new AtomicBoolean(false);
        AtomicBoolean newConnectionAborted = new AtomicBoolean(false);
        Connection oldConnection = connection(oldConnectionAborted);
        Connection newConnection = connection(newConnectionAborted);
        AtomicInteger connectionCount = new AtomicInteger();
        JdbcConfiguration configuration =
                JdbcConfiguration.adapt(
                        Configuration.empty()
                                .edit()
                                .with("ApplicationName", PostgresConnection.CONNECTION_GENERAL)
                                .build());
        JdbcConnection jdbcConnection =
                new JdbcConnection(
                        configuration,
                        TrackingPostgresConnection.trackingFactory(
                                config ->
                                        connectionCount.getAndIncrement() == 0
                                                ? oldConnection
                                                : newConnection),
                        "\"",
                        "\"");

        try (PostgresConnection.ConnectionTrackingScope ignored =
                PostgresConnection.trackConnections(
                        PostgresConnection.CONNECTION_GENERAL, abortableConnection::capture)) {
            assertSame(oldConnection, jdbcConnection.connection(false));
            abortableConnection.abort(Runnable::run);
            try {
                jdbcConnection.prepareQuery("SELECT 1");
                throw new AssertionError("Expected a replacement connection to be rejected");
            } catch (ConnectException expected) {
                assertTrue(expected.getCause() instanceof SQLException);
                assertEquals(
                        "Connection opened during forced shutdown",
                        expected.getCause().getMessage());
            }
        }

        assertEquals(2, connectionCount.get());
        assertTrue(oldConnectionAborted.get());
        assertTrue(newConnectionAborted.get());
    }

    private static ReplicationConnection replicationConnection(AtomicBoolean closed) {
        return (ReplicationConnection)
                Proxy.newProxyInstance(
                        ReplicationConnection.class.getClassLoader(),
                        new Class<?>[] {ReplicationConnection.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("close")) {
                                closed.set(true);
                            }
                            return null;
                        });
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

        TestJdbcConnection(boolean failCommit) {
            this(new AtomicBoolean(false), new AtomicBoolean(false), failCommit);
        }

        private TestJdbcConnection(
                AtomicBoolean connected, AtomicBoolean committed, boolean failCommit) {
            super(
                    JdbcConfiguration.empty(),
                    config -> {
                        connected.set(true);
                        return commitConnection(committed, failCommit);
                    },
                    "\"",
                    "\"");
            this.connected = connected;
            this.committed = committed;
        }

        private static Connection commitConnection(AtomicBoolean committed, boolean failCommit) {
            return (Connection)
                    Proxy.newProxyInstance(
                            Connection.class.getClassLoader(),
                            new Class<?>[] {Connection.class},
                            (proxy, method, args) -> {
                                switch (method.getName()) {
                                    case "isClosed":
                                    case "getAutoCommit":
                                        return false;
                                    case "commit":
                                        committed.set(true);
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

    private static class TrackingPostgresConnection extends PostgresConnection {
        private TrackingPostgresConnection() {
            super(JdbcConfiguration.empty(), CONNECTION_GENERAL);
        }

        private static JdbcConnection.ConnectionFactory trackingFactory(
                JdbcConnection.ConnectionFactory delegate) {
            return PostgresConnection.trackCreatedConnections(delegate);
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
