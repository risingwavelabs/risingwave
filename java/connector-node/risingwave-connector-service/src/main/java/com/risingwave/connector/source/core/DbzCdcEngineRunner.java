// Copyright 2023 RisingWave Labs
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

import com.risingwave.connector.api.source.*;
import com.risingwave.connector.source.common.DbzConnectorConfig;
import com.risingwave.proto.ConnectorServiceProto.GetEventStreamResponse;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Single-thread engine runner */
public class DbzCdcEngineRunner implements CdcEngineRunner {
    static final Logger LOG = LoggerFactory.getLogger(DbzCdcEngineRunner.class);

    private final ExecutorService executor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final CdcEngine engine;

    private DbzCdcEngineRunner(CdcEngine engine) {
        this.executor =
                Executors.newSingleThreadExecutor(
                        r -> new Thread(r, "rw-dbz-engine-runner-" + engine.getId()));
        this.engine = engine;
    }

    public static CdcEngineRunner newCdcEngineRunner(
            DbzConnectorConfig config, StreamObserver<GetEventStreamResponse> responseObserver) {
        DbzCdcEngineRunner runner = null;
        try {
            var sourceId = config.getSourceId();
            var engine =
                    new DbzCdcEngine(
                            config.getSourceId(),
                            config.getResolvedDebeziumProps(),
                            (success, message, error) -> {
                                if (!success) {
                                    responseObserver.onError(error);
                                    LOG.error(
                                            "engine#{} terminated with error. message: {}",
                                            sourceId,
                                            message,
                                            error);
                                } else {
                                    LOG.info("engine#{} stopped normally. {}", sourceId, message);
                                    responseObserver.onCompleted();
                                }
                            });

            runner = new DbzCdcEngineRunner(engine);
        } catch (Exception e) {
            LOG.error("failed to create the CDC engine", e);
        }
        return runner;
    }

    public static CdcEngineRunner newCdcEngineRunner(DbzConnectorConfig config) {
        DbzCdcEngineRunner runner = null;
        try {
            var sourceId = config.getSourceId();
            var engine =
                    new DbzCdcEngine(
                            config.getSourceId(),
                            config.getResolvedDebeziumProps(),
                            (success, message, error) -> {
                                if (!success) {
                                    LOG.error(
                                            "engine#{} terminated with error. message: {}",
                                            sourceId,
                                            message,
                                            error);
                                } else {
                                    LOG.info("engine#{} stopped normally. {}", sourceId, message);
                                }
                            });

            runner = new DbzCdcEngineRunner(engine);
        } catch (Exception e) {
            LOG.error("failed to create the CDC engine", e);
        }
        return runner;
    }

    /** Start to run the cdc engine */
    public void start() throws InterruptedException {
        if (isRunning()) {
            LOG.info("engine#{} already started", engine.getId());
            return;
        }

        // put a handshake message to notify the Source executor
        var controlInfo =
                GetEventStreamResponse.ControlInfo.newBuilder().setHandshakeOk(true).build();
        engine.getOutputChannel()
                .put(
                        GetEventStreamResponse.newBuilder()
                                .setSourceId(engine.getId())
                                .setControl(controlInfo)
                                .build());
        executor.execute(engine);
        running.set(true);
        LOG.info("engine#{} started", engine.getId());
    }

    public void stop() throws Exception {
        if (isRunning()) {
            engine.stop();
            cleanUp();
            LOG.info("engine#{} terminated", engine.getId());
        }
    }

    @Override
    public CdcEngine getEngine() {
        return engine;
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    private void cleanUp() {
        running.set(false);
        // interrupt the runner thread if it is still running
        executor.shutdownNow();
    }
}
