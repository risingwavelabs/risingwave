// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.risingwave.connector.source.core;

import com.risingwave.connector.source.common.DbzConnectorConfig;
import com.risingwave.connector.source.common.DbzSourceUtils;
import com.risingwave.java.binding.CdcSourceChannel;
import com.risingwave.proto.ConnectorServiceProto.GetEventStreamResponse;
import io.debezium.config.CommonConnectorConfig;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Single-thread engine runner */
public class DbzCdcEngineRunner {
    static final Logger LOG = LoggerFactory.getLogger(DbzCdcEngineRunner.class);

    private final ExecutorService executor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private DbzCdcEngine engine;
    private final DbzConnectorConfig config;

    public static DbzCdcEngineRunner newCdcEngineRunner(
            DbzConnectorConfig config, StreamObserver<GetEventStreamResponse> responseObserver) {
        DbzCdcEngineRunner runner = null;
        try {
            var sourceId = config.getSourceId();
            var engine =
                    new DbzCdcEngine(
                            config.getSourceType(),
                            config.getSourceId(),
                            config.getResolvedDebeziumProps(),
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

    public static DbzCdcEngineRunner create(DbzConnectorConfig config, CdcSourceChannel channel) {
        DbzCdcEngineRunner runner = new DbzCdcEngineRunner(config);
        try {
            var sourceId = config.getSourceId();
            final DbzCdcEngineRunner finalRunner = runner;
            var engine =
                    new DbzCdcEngine(
                            config.getSourceType(),
                            config.getSourceId(),
                            config.getResolvedDebeziumProps(),
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
                                        finalRunner.stop();
                                    } catch (Exception e) {
                                        LOG.warn("failed to stop the engine#{}", sourceId, e);
                                    }
                                } else {
                                    LOG.info("engine#{} stopped normally. {}", sourceId, message);
                                }
                            });

            runner.withEngine(engine);
        } catch (Exception e) {
            LOG.error("failed to create the CDC engine", e);
            runner = null;
        }
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
    }

    /** Start to run the cdc engine */
    public boolean start() throws InterruptedException {
        if (isRunning()) {
            LOG.info("engine#{} already started", engine.getId());
            return true;
        }

        executor.execute(engine);

        boolean startOk = true;
        // For backfill source, we need to wait for the streaming source to start before proceeding
        if (config.isBackfillSource()) {
            var databaseServerName =
                    config.getResolvedDebeziumProps()
                            .getProperty(CommonConnectorConfig.TOPIC_PREFIX.name());
            startOk =
                    DbzSourceUtils.waitForStreamingRunning(
                            config.getSourceType(),
                            databaseServerName,
                            config.getWaitStreamingStartTimeout());
        }

        running.set(true);
        LOG.info("engine#{} start ok: {}", engine.getId(), startOk);
        return startOk;
    }

    public void stop() throws Exception {
        if (isRunning()) {
            engine.stop();
            cleanUp();
            LOG.info("engine#{} terminated", engine.getId());
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

    private void cleanUp() {
        running.set(false);
        // interrupt the runner thread if it is still running
        executor.shutdownNow();
    }
}
