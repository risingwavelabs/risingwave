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

import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;

import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

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
        stopEngineBeforeStreaming(host, port, user, password, url);
    }

    private static void stopEngineBeforeStreaming(
            String host, String port, String user, String password, String url) throws Exception {
        var directory = Files.createTempDirectory("sqlserver-engine-stop-");
        String applicationName =
                "rw_shutdown_" + java.util.UUID.randomUUID().toString().replace("-", "");
        String database = applicationName;
        try (Connection control = DriverManager.getConnection(url, user, password);
                Statement statement = control.createStatement()) {
            statement.execute("CREATE DATABASE " + database);
            statement.execute("USE " + database);
            statement.execute("EXEC sys.sp_cdc_enable_db");
            statement.execute("CREATE TABLE dbo.items (id INT PRIMARY KEY)");
            statement.execute(
                    "EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'items',"
                        + " @role_name = NULL");
        }
        var props = new Properties();
        props.setProperty("name", applicationName);
        props.setProperty("topic.prefix", applicationName);
        props.setProperty("connector.class", SqlServerConnector.class.getName());
        props.setProperty("database.hostname", host);
        props.setProperty("database.port", port);
        props.setProperty("database.user", user);
        props.setProperty("database.password", password);
        props.setProperty("database.names", database);
        props.setProperty("database.encrypt", "true");
        props.setProperty("database.trustServerCertificate", "true");
        props.setProperty("driver.applicationName", applicationName);
        props.setProperty("snapshot.mode", "no_data");
        props.setProperty("snapshot.delay.ms", "5000");
        props.setProperty("snapshot.lock.timeout.ms", "600000");
        props.setProperty("executor.shutdown.timeout.ms", "1000");
        props.setProperty(
                "offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", directory.resolve("offsets").toString());
        props.setProperty(
                "schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory");
        props.setProperty(
                "schema.history.internal.file.filename", directory.resolve("history").toString());
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        var engine =
                DebeziumEngine.create(Connect.class)
                        .using(props)
                        .using(
                                new DebeziumEngine.ConnectorCallback() {
                                    @Override
                                    public void taskStarted() {
                                        taskStarted.countDown();
                                    }
                                })
                        .using(
                                (success, message, error) -> {
                                    if (!success) {
                                        failure.set(
                                                error == null
                                                        ? new AssertionError(message)
                                                        : error);
                                    }
                                    completed.countDown();
                                })
                        .notifying(record -> {})
                        .build();
        ExecutorService executor =
                Executors.newFixedThreadPool(
                        2,
                        command -> {
                            Thread thread = new Thread(command, "sqlserver-engine-stop-test");
                            thread.setDaemon(true);
                            return thread;
                        });
        try (Connection control = DriverManager.getConnection(url, user, password);
                Connection blocker = DriverManager.getConnection(url, user, password)) {
            executor.submit(engine);
            if (!taskStarted.await(30, TimeUnit.SECONDS)) {
                throw new AssertionError("engine task did not start", failure.get());
            }
            int blockerId = sessionIdAndAssertEncrypted(blocker);
            try (Statement statement = blocker.createStatement()) {
                statement.execute("USE " + database);
                blocker.setAutoCommit(false);
                // Hold a schema modification lock while the snapshot tries to discover tables.
                statement.execute("ALTER TABLE dbo.items ADD blocked INT NULL");
            }
            int sessionId = waitForEngineRequest(control, applicationName, blockerId);
            // Exercise the real engine -> task -> coordinator stop path, without calling abort.
            executor.submit(
                            () -> {
                                engine.close();
                                return null;
                            })
                    .get(15, TimeUnit.SECONDS);
            if (!completed.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("engine did not complete after stop");
            }
            if (failure.get() != null) {
                throw new AssertionError("engine cleanup failed", failure.get());
            }
            waitForSessionClosed(control, sessionId);
            waitForEngineSessionsClosed(control, applicationName);
            blocker.rollback();
        } finally {
            executor.shutdownNow();
            try (var files = Files.list(directory)) {
                for (var file : files.toList()) {
                    Files.deleteIfExists(file);
                }
            }
            Files.deleteIfExists(directory);
            try (Connection control = DriverManager.getConnection(url, user, password);
                    Statement statement = control.createStatement()) {
                statement.execute(
                        "ALTER DATABASE " + database + " SET SINGLE_USER WITH ROLLBACK IMMEDIATE");
                statement.execute("DROP DATABASE " + database);
            }
        }
    }

    private static void waitForEngineSessionsClosed(Connection control, String applicationName)
            throws Exception {
        long deadline = System.nanoTime() + READ_UNBLOCK_TIMEOUT.toNanos();
        try (PreparedStatement statement =
                control.prepareStatement(
                        "SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE program_name = ?")) {
            statement.setString(1, applicationName);
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
        throw new AssertionError("engine sessions survived stop");
    }

    private static int waitForEngineRequest(
            Connection control, String applicationName, int blockerId) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        try (PreparedStatement statement =
                control.prepareStatement(
                        "SELECT s.session_id, c.encrypt_option FROM sys.dm_exec_sessions s "
                                + "JOIN sys.dm_exec_connections c ON c.session_id = s.session_id "
                                + "JOIN sys.dm_exec_requests r ON r.session_id = s.session_id "
                                + "WHERE s.program_name = ? AND r.blocking_session_id = ?")) {
            statement.setString(1, applicationName);
            statement.setInt(2, blockerId);
            while (System.nanoTime() < deadline) {
                try (ResultSet result = statement.executeQuery()) {
                    if (result.next()) {
                        if (!"TRUE".equalsIgnoreCase(result.getString(2))) {
                            throw new AssertionError("engine connection is not encrypted");
                        }
                        return result.getInt(1);
                    }
                }
                Thread.sleep(100);
            }
        }
        throw new AssertionError("engine did not reach pre-streaming blocked I/O");
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
