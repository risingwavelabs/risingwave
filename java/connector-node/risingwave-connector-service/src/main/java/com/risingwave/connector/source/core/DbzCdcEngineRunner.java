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
import com.risingwave.java.binding.CdcSourceChannel;
import com.risingwave.proto.ConnectorServiceProto.GetEventStreamResponse;
import io.debezium.config.CommonConnectorConfig;
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
    private DbzCdcEngine engine;
    private final CdcSource source;

    public static DbzCdcEngineRunner newCdcEngineRunner(
            CdcSource source, StreamObserver<GetEventStreamResponse> responseObserver) {
        DbzCdcEngineRunner runner = null;
        try {
            var sourceId = source.getSourceId();
            var engine =
                    new DbzCdcEngine(
                            source,
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

            runner = new DbzCdcEngineRunner(source);
            runner.withEngine(engine);
        } catch (Exception e) {
            LOG.error("failed to create the CDC engine", e);
        }
        return runner;
    }

    /**
     * Creates the runner for the JNI path. A failure to build the engine propagates to the caller.
     */
    public static DbzCdcEngineRunner create(CdcSource source, CdcSourceChannel channel) {
        var runner = new DbzCdcEngineRunner(source);
        var sourceId = source.getSourceId();
        var engine =
                new DbzCdcEngine(
                        source,
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
                                // We need to stop the engine runner on debezium engine failure
                                try {
                                    runner.stop();
                                } catch (Exception e) {
                                    LOG.warn("failed to stop the engine#{}", sourceId, e);
                                }
                            } else {
                                LOG.info("engine#{} stopped normally. {}", sourceId, message);
                            }
                        });

        runner.withEngine(engine);
        return runner;
    }

    // private constructor
    private DbzCdcEngineRunner(CdcSource source) {
        this.executor =
                Executors.newSingleThreadExecutor(
                        r -> new Thread(r, "rw-dbz-engine-runner-" + source.getSourceId()));
        this.source = source;
    }

    private void withEngine(DbzCdcEngine engine) {
        this.engine = engine;
    }

    DbzCdcEngineRunner(CdcSource source, DbzCdcEngine engine) {
        this(source);
        this.engine = engine;
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
        if (source.isBackfillSource()) {
            var databaseServerName =
                    source.getConfig()
                            .getResolvedDebeziumProps()
                            .getProperty(CommonConnectorConfig.TOPIC_PREFIX.name());
            startOk =
                    source.waitForStreamingRunning(
                            databaseServerName,
                            source.getWaitStreamingStartTimeout(),
                            () -> isRunning() && shouldContinue.getAsBoolean());
        }

        startOk = startOk && isRunning();
        LOG.info("engine#{} start ok: {}", engine.getId(), startOk);
        return startOk;
    }

    public void stop() throws Exception {
        if (running.compareAndSet(true, false)) {
            try {
                engine.stop();
                LOG.info("engine#{} terminated", engine.getId());
            } finally {
                // Interrupt the runner thread even if closing the engine fails.
                executor.shutdownNow();
            }
        }
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
