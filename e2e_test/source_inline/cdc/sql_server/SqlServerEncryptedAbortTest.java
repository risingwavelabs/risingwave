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

package io.debezium.connector.sqlserver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Regression test for aborting an encrypted connection blocked in a socket read. */
public final class SqlServerEncryptedAbortTest {
    private static final Duration REQUEST_START_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration ABORT_RETURN_TIMEOUT = Duration.ofSeconds(2);
    private static final Duration READ_UNBLOCK_TIMEOUT = Duration.ofSeconds(10);

    private SqlServerEncryptedAbortTest() {}

    public static void main(String[] args) throws Exception {
        String host = envOrDefault("SQLCMDSERVER", "sqlserver-server");
        String port = envOrDefault("SQLCMDPORT", "1433");
        String user = envOrDefault("SQLCMDUSER", "SA");
        String password = requiredEnv("SQLCMDPASSWORD");
        String url =
                "jdbc:sqlserver://"
                        + host
                        + ":"
                        + port
                        + ";databaseName=master;encrypt=true;trustServerCertificate=true";

        ExecutorService queryExecutor =
                Executors.newSingleThreadExecutor(
                        command -> {
                            Thread thread = new Thread(command, "sqlserver-blocked-read-test");
                            thread.setDaemon(true);
                            return thread;
                        });
        Connection blocked = DriverManager.getConnection(url, user, password);
        try (Connection control = DriverManager.getConnection(url, user, password)) {
            int sessionId = sessionIdAndAssertEncrypted(blocked);
            Future<?> blockedRead =
                    queryExecutor.submit(
                            () -> {
                                try (Statement statement = blocked.createStatement()) {
                                    statement.execute("WAITFOR DELAY '00:10:00'");
                                }
                                return null;
                            });

            waitForRequest(control, sessionId);

            long startedAt = System.nanoTime();
            SqlServerStreamingChangeEventSource.abortConnection(blocked, "test");
            Duration abortDuration = Duration.ofNanos(System.nanoTime() - startedAt);
            if (abortDuration.compareTo(ABORT_RETURN_TIMEOUT) > 0) {
                throw new AssertionError("abort blocked for " + abortDuration);
            }

            assertReadUnblocked(blockedRead);
            waitForSessionClosed(control, sessionId);
        } finally {
            queryExecutor.shutdownNow();
        }
    }

    private static int sessionIdAndAssertEncrypted(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet result =
                        statement.executeQuery(
                                "SELECT @@SPID, encrypt_option "
                                        + "FROM sys.dm_exec_connections "
                                        + "WHERE session_id = @@SPID")) {
            if (!result.next()) {
                throw new AssertionError("SQL Server connection was not visible");
            }
            if (!"TRUE".equalsIgnoreCase(result.getString(2))) {
                throw new AssertionError("test connection is not encrypted");
            }
            return result.getInt(1);
        }
    }

    private static void waitForRequest(Connection control, int sessionId) throws Exception {
        long deadline = System.nanoTime() + REQUEST_START_TIMEOUT.toNanos();
        try (PreparedStatement statement =
                control.prepareStatement(
                        "SELECT COUNT(*) FROM sys.dm_exec_requests WHERE session_id = ?")) {
            statement.setInt(1, sessionId);
            while (System.nanoTime() < deadline) {
                try (ResultSet result = statement.executeQuery()) {
                    result.next();
                    if (result.getInt(1) == 1) {
                        return;
                    }
                }
                Thread.sleep(100);
            }
        }
        throw new AssertionError("timed out waiting for the blocked SQL Server request");
    }

    private static void waitForSessionClosed(Connection control, int sessionId) throws Exception {
        long deadline = System.nanoTime() + READ_UNBLOCK_TIMEOUT.toNanos();
        try (PreparedStatement statement =
                control.prepareStatement(
                        "SELECT COUNT(*) FROM sys.dm_exec_connections WHERE session_id = ?")) {
            statement.setInt(1, sessionId);
            while (System.nanoTime() < deadline) {
                try (ResultSet result = statement.executeQuery()) {
                    result.next();
                    if (result.getInt(1) == 0) {
                        return;
                    }
                }
                Thread.sleep(100);
            }
        }
        throw new AssertionError("aborted SQL Server session was not released");
    }

    private static void assertReadUnblocked(Future<?> blockedRead) throws Exception {
        try {
            blockedRead.get(READ_UNBLOCK_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            throw new AssertionError("blocked SQL Server request completed without an abort error");
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof SQLException)) {
                throw e;
            }
        } catch (TimeoutException e) {
            throw new AssertionError("abort did not unblock the encrypted socket read", e);
        }
    }

    private static String envOrDefault(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static String requiredEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must be set");
        }
        return value;
    }
}
