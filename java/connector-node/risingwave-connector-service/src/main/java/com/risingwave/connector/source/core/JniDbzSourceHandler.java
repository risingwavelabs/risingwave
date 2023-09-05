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

import com.risingwave.connector.api.source.CdcEngineRunner;
import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.connector.source.common.DbzConnectorConfig;
import com.risingwave.java.binding.Binding;
import com.risingwave.metrics.ConnectorNodeMetrics;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** handler for starting a debezium source connectors for jni */
public class JniDbzSourceHandler {
    static final Logger LOG = LoggerFactory.getLogger(DbzSourceHandler.class);

    private final DbzConnectorConfig config;

    public JniDbzSourceHandler(DbzConnectorConfig config) {
        this.config = config;
    }

    public static void runJniDbzSourceThread(
            SourceTypeE source,
            long sourceId,
            String startOffset,
            Map<String, String> userProps,
            boolean snapshotDone,
            long channelPtr) {
        // For jni.rs
        java.lang.Thread.currentThread()
                .setContextClassLoader(java.lang.ClassLoader.getSystemClassLoader());
        // userProps extracted from grpc request, underlying implementation is UnmodifiableMap
        Map<String, String> mutableUserProps = new HashMap<>(userProps);
        mutableUserProps.put("source.id", Long.toString(sourceId));
        var config =
                new DbzConnectorConfig(
                        source, sourceId, startOffset, mutableUserProps, snapshotDone);
        JniDbzSourceHandler handler = new JniDbzSourceHandler(config);
        handler.start(channelPtr);
    }

    class OnReadyHandler implements Runnable {
        private final CdcEngineRunner runner;
        private final long channelPtr;

        public OnReadyHandler(CdcEngineRunner runner, long channelPtr) {
            this.runner = runner;
            this.channelPtr = channelPtr;
        }

        @Override
        public void run() {
            while (runner.isRunning()) {
                try {
                    // check whether the send queue has room for new messages
                    // Thread will block on the channel to get output from engine
                    var resp =
                            runner.getEngine().getOutputChannel().poll(500, TimeUnit.MILLISECONDS);
                    boolean success;
                    if (resp != null) {
                        ConnectorNodeMetrics.incSourceRowsReceived(
                                config.getSourceType().toString(),
                                String.valueOf(config.getSourceId()),
                                resp.getEventsCount());
                        LOG.info(
                                "Engine#{}: emit one chunk {} events to network ",
                                config.getSourceId(),
                                resp.getEventsCount());
                        success = Binding.sendCdcSourceMsgToChannel(channelPtr, resp.toByteArray());
                    } else {
                        // If resp is null means just check whether channel is closed.
                        success = Binding.sendCdcSourceMsgToChannel(channelPtr, null);
                    }
                    if (!success) {
                        LOG.info(
                                "Engine#{}: JNI sender broken detected, stop the engine",
                                config.getSourceId());
                        runner.stop();
                        return;
                    }
                } catch (Throwable e) {
                    LOG.error("Poll engine output channel fail. ", e);
                }
            }
        }
    }

    public void start(long channelPtr) {
        var runner = DbzCdcEngineRunner.newCdcEngineRunner(config);
        if (runner == null) {
            return;
        }

        try {
            // Start the engine
            runner.start();
            LOG.info("Start consuming events of table {}", config.getSourceId());

            final OnReadyHandler onReadyHandler = new OnReadyHandler(runner, channelPtr);

            onReadyHandler.run();

        } catch (Throwable t) {
            LOG.error("Cdc engine failed.", t);
            try {
                runner.stop();
            } catch (Exception e) {
                LOG.warn("Failed to stop Engine#{}", config.getSourceId(), e);
            }
        }
    }
}
