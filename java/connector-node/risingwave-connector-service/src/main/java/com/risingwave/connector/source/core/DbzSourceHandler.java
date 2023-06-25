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
import com.risingwave.connector.api.source.SourceHandler;
import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.connector.source.common.DbzConnectorConfig;
import com.risingwave.connector.source.common.ValidatorUtils;
import com.risingwave.metrics.ConnectorNodeMetrics;
import com.risingwave.proto.ConnectorServiceProto.GetEventStreamResponse;
import io.grpc.Context;
import io.grpc.stub.ServerCallStreamObserver;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** * handler for starting a debezium source connectors */
public class DbzSourceHandler implements SourceHandler {
    static final Logger LOG = LoggerFactory.getLogger(DbzSourceHandler.class);

    private Map<Long, String> replicationSlotMap;

    private final DbzConnectorConfig config;

    public DbzSourceHandler(Map<Long, String> replicationSlotMap, DbzConnectorConfig config) {
        this.replicationSlotMap = replicationSlotMap;
        this.config = config;
    }

    class OnReadyHandler implements Runnable {
        private final CdcEngineRunner runner;
        private final ServerCallStreamObserver<GetEventStreamResponse> responseObserver;

        public OnReadyHandler(
                CdcEngineRunner runner,
                ServerCallStreamObserver<GetEventStreamResponse> responseObserver) {
            this.runner = runner;
            this.responseObserver = responseObserver;
        }

        @Override
        public void run() {
            while (runner.isRunning()) {
                try {
                    if (Context.current().isCancelled()) {
                        LOG.info(
                                "Engine#{}: Connection broken detected, stop the engine",
                                config.getSourceId());
                        runner.stop();
                        if (config.getSourceType() == SourceTypeE.POSTGRES) {
                            String slotName = replicationSlotMap.get(config.getSourceId());
                            if (slotName != null) {
                                dropReplicationSlot(config, slotName);
                            }
                        }
                        return;
                    }
                    // check whether the send queue has room for new messages
                    if (responseObserver.isReady()) {
                        // Thread will block on the channel to get output from engine
                        var resp =
                                runner.getEngine()
                                        .getOutputChannel()
                                        .poll(500, TimeUnit.MILLISECONDS);
                        if (resp != null) {
                            ConnectorNodeMetrics.incSourceRowsReceived(
                                    config.getSourceType().toString(),
                                    String.valueOf(config.getSourceId()),
                                    resp.getEventsCount());
                            LOG.debug(
                                    "Engine#{}: emit one chunk {} events to network ",
                                    config.getSourceId(),
                                    resp.getEventsCount());
                            responseObserver.onNext(resp);
                        }
                    } else { // back pressure detected, return to avoid oom
                        return;
                    }
                } catch (Exception e) {
                    LOG.error("Poll engine output channel fail. ", e);
                }
            }
        }
    }

    @Override
    public void startSource(ServerCallStreamObserver<GetEventStreamResponse> responseObserver) {
        var runner = DbzCdcEngineRunner.newCdcEngineRunner(config, responseObserver);
        if (runner == null) {
            responseObserver.onCompleted();
            return;
        }

        try {
            // Start the engine
            runner.start();
            LOG.info("Start consuming events of table {}", config.getSourceId());

            final OnReadyHandler onReadyHandler = new OnReadyHandler(runner, responseObserver);

            responseObserver.disableAutoRequest();
            responseObserver.setOnReadyHandler(onReadyHandler);

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

    private void dropReplicationSlot(DbzConnectorConfig config, String slotName) throws Exception {
        String dbHost = config.getPropNotNull(DbzConnectorConfig.HOST);
        String dbPort = config.getPropNotNull(DbzConnectorConfig.PORT);
        String dbName = config.getPropNotNull(DbzConnectorConfig.DB_NAME);
        String jdbcUrl = ValidatorUtils.getJdbcUrl(SourceTypeE.POSTGRES, dbHost, dbPort, dbName);

        String user = config.getPropNotNull(DbzConnectorConfig.USER);
        String password = config.getPropNotNull(DbzConnectorConfig.PASSWORD);
        Connection jdbcConnection = DriverManager.getConnection(jdbcUrl, user, password);
        // check if replication slot used by active process
        try (var stmt0 =
                jdbcConnection.prepareStatement(
                        "select active_pid from pg_replication_slots where slot_name = ?")) {
            stmt0.setString(1, slotName);
            var res = stmt0.executeQuery();
            if (res.next()) {
                int pid = res.getInt(1);
                if (res.next()) {
                    // replication slot used by multiple process, cannot drop
                    throw ValidatorUtils.internalError(
                            "cannot drop replication slot "
                                    + slotName
                                    + "because it is used by multiple active postgres processes");
                }
                // replication slot is used by only one process, as expected
                // terminate this process
                try (var stmt1 =
                        jdbcConnection.prepareStatement("select pg_terminate_backend(?)")) {
                    stmt1.setInt(1, pid);
                    stmt1.executeQuery();
                }
            }
        }
        // drop the replication slot, which should now be inactive
        try (var stmt = jdbcConnection.prepareStatement("select pg_drop_replication_slot(?)")) {
            stmt.setString(1, slotName);
            stmt.executeQuery();
        }
    }
}
