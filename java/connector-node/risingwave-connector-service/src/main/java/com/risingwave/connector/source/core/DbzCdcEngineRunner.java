/*
 * Copyright 2023 RisingWave Labs
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

import com.risingwave.connector.source.common.CdcSource;
import com.risingwave.connector.source.common.DbzConnectorConfig;
import com.risingwave.java.binding.CdcSourceChannel;
import com.risingwave.proto.ConnectorServiceProto.GetEventStreamResponse;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Single-thread engine runner */
public class DbzCdcEngineRunner {
    static final Logger LOG = LoggerFactory.getLogger(DbzCdcEngineRunner.class);

    private final ExecutorService executor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private volatile Exception stopFailure;
    private DbzCdcEngine engine;
    private final DbzConnectorConfig config;

    public static DbzCdcEngineRunner newCdcEngineRunner(
            CdcSource source,
            DbzConnectorConfig config,
            StreamObserver<GetEventStreamResponse> responseObserver) {
        DbzCdcEngineRunner runner = null;
        try {
            var sourceId = config.getSourceId();
            var engine =
                    new DbzCdcEngine(
                            source,
                            config,
                            (success, message, error) -> {
                                if (!success) {
                                    LOG.error(
                                            "engine#{} terminated with error. message: {}",
                                            sourceId,
                                            message,
                                            error);
                                    if (error != null) {
                                        responseObserver.onError(error);
                                    }
                                } else {
                                    LOG.info("engine#{} stopped normally. {}", sourceId, message);
                                    responseObserver.onCompleted();
                                }
                            });

            runner = new DbzCdcEngineRunner(config);
            runner.withEngine(engine);
        } catch (Exception e) {
            LOG.error("failed to create the CDC engine", e);
        }
        return runner;
    }

    /**
     * Creates the runner for the JNI path. A failure to build the engine propagates to the caller.
     */
    public static DbzCdcEngineRunner create(
            CdcSource source, DbzConnectorConfig config, CdcSourceChannel channel) {
        var runner = new DbzCdcEngineRunner(config);
        var sourceId = config.getSourceId();
        var engine =
                new DbzCdcEngine(
                        source,
                        config,
                        (success, message, error) -> {
                            if (!success) {
                                LOG.error(
                                        "engine#{} terminated with error. message: {}",
                                        sourceId,
                                        message,
                                        error);
                                String errorMsg =
                                        (error != null && error.getMessage() != null
                                                ? error.getMessage()
                                                : message);
                                if (!channel.sendError(errorMsg)) {
                                    LOG.warn(
                                            "engine#{} unable to send error message: {}",
                                            sourceId,
                                            errorMsg);
                                }
                                // onCompletion ends the run loop. The JNI handler retries cleanup,
                                // so this callback only reports the failure.
                            } else {
                                LOG.info("engine#{} stopped normally. {}", sourceId, message);
                            }
                        });

        runner.withEngine(engine);
        return runner;
    }

    // private constructor
    private DbzCdcEngineRunner(DbzConnectorConfig config) {
        this.executor =
                Executors.newSingleThreadExecutor(
                        r -> new Thread(r, "rw-dbz-engine-runner-" + config.getSourceId()));
        this.config = config;
    }

    private void withEngine(DbzCdcEngine engine) {
        this.engine = engine;
        engine.onCompletion(
                () -> {
                    running.set(false);
                    executor.shutdown();
                });
    }

    DbzCdcEngineRunner(DbzConnectorConfig config, DbzCdcEngine engine) {
        this(config);
        withEngine(engine);
    }

    /** Start to run the cdc engine */
    public boolean start() throws InterruptedException {
        return start(() -> true);
    }

    public boolean start(BooleanSupplier shouldContinue) throws InterruptedException {
        // The engine must be stoppable as soon as it is submitted, including while waiting
        // for streaming readiness. A completion callback may also stop it during startup.
        if (!running.compareAndSet(false, true)) {
            LOG.info("engine#{} already started", engine.getId());
            return true;
        }

        executor.execute(engine);

        boolean startOk = true;
        // For backfill source, we need to wait for the streaming source to start before proceeding
        if (config.isBackfillSource()) {
            startOk =
                    config.waitForStreamingRunning(
                            () -> isRunning() && shouldContinue.getAsBoolean());
        }

        startOk = startOk && isRunning();
        LOG.info("engine#{} start ok: {}", engine.getId(), startOk);
        return startOk;
    }

    public void stop() throws Exception {
        if (!stopping.compareAndSet(false, true)) {
            return;
        }
        try {
            if (running.getAndSet(false) || getStopFailure() != null) {
                try {
                    engine.stop();
                    stopFailure = null;
                    LOG.info("engine#{} terminated", engine.getId());
                } catch (Exception e) {
                    stopFailure = e;
                    LOG.error(
                            "engine#{} cleanup failed; a subsequent stop will retry",
                            engine.getId(),
                            e);
                    throw e;
                } finally {
                    // Interrupt the runner thread even if closing the engine fails.
                    executor.shutdownNow();
                }
            }
        } finally {
            stopping.set(false);
        }
    }

    /** A stopped running loop does not imply successful resource cleanup. */
    public Exception getStopFailure() {
        // Completion callbacks can report failures after stop() returned. Consult the engine's
        // live task-cleanup state rather than treating the return from close() as success.
        Exception cleanupFailure = engine.getCleanupFailure();
        return cleanupFailure != null ? cleanupFailure : stopFailure;
    }

    public DbzCdcEngine getEngine() {
        return engine;
    }

    public boolean isRunning() {
        return running.get();
    }

    public DbzChangeEventConsumer getChangeEventConsumer() {
        return engine.getChangeEventConsumer();
    }
}
