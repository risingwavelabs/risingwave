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
    public void cleanupSkipsCommitAfterForcedShutdown() {
        TestJdbcConnection connection = new TestJdbcConnection(false);
        AtomicBoolean replicationClosed = new AtomicBoolean(false);

        PostgresStreamingChangeEventSource.cleanUpConnectionOnStop(
                connection, replicationConnection(replicationClosed), false);

        assertFalse(connection.committed.get());
        assertTrue(replicationClosed.get());
    }

    @Test
    public void cleanupClosesReplicationConnectionWhenCommitFails() {
        TestJdbcConnection connection = new TestJdbcConnection(true);
        AtomicBoolean replicationClosed = new AtomicBoolean(false);

        PostgresStreamingChangeEventSource.cleanUpConnectionOnStop(
                connection, replicationConnection(replicationClosed), true);

        assertTrue(connection.committed.get());
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
                            if (method.getName().equals("abort")) {
                                aborted.set(true);
                            }
                            return null;
                        });
    }

    private static class TestJdbcConnection extends JdbcConnection {
        private final AtomicBoolean committed = new AtomicBoolean(false);
        private final boolean failCommit;

        TestJdbcConnection(boolean failCommit) {
            super(
                    JdbcConfiguration.empty(),
                    config -> {
                        throw new AssertionError("A test connection should not be established");
                    },
                    "\"",
                    "\"");
            this.failCommit = failCommit;
        }

        @Override
        public JdbcConnection commit() throws SQLException {
            committed.set(true);
            if (failCommit) {
                throw new SQLException("commit failed");
            }
            return this;
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
