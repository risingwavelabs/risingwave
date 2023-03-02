package com.risingwave.sourcenode.core;

import com.risingwave.connector.api.source.SourceConfig;
import com.risingwave.connector.api.source.SourceHandler;
import com.risingwave.proto.ConnectorServiceProto.GetEventStreamResponse;
import io.grpc.Context;
import io.grpc.stub.ServerCallStreamObserver;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Default handler for RPC request */
public class DefaultSourceHandler implements SourceHandler {
    static final Logger LOG = LoggerFactory.getLogger(DefaultSourceHandler.class);

    private final SourceConfig config;

    private DefaultSourceHandler(SourceConfig config) {
        this.config = config;
    }

    public static DefaultSourceHandler newWithConfig(SourceConfig config) {
        return new DefaultSourceHandler(config);
    }

    @Override
    public void handle(ServerCallStreamObserver<GetEventStreamResponse> responseObserver) {
        var runner =
                DefaultCdcEngineRunner.newCdcEngineRunner(config.getId(), config, responseObserver);
        if (runner == null) {
            responseObserver.onCompleted();
            return;
        }

        try {
            // Start the engine
            runner.start();
            LOG.info("Start consuming events of table {}", config.getId());
            while (runner.isRunning()) {
                try {
                    // Thread will block on the channel to get output from engine
                    var resp =
                            runner.getEngine().getOutputChannel().poll(500, TimeUnit.MILLISECONDS);

                    if (resp != null) {
                        // check whether the send queue has room for new messages
                        while (!responseObserver.isReady() && !Context.current().isCancelled()) {
                            // wait a bit to avoid OOM
                            Thread.sleep(500);
                        }

                        LOG.debug(
                                "Engine#{}: emit one chunk {} events to network ",
                                config.getId(),
                                resp.getEventsCount());
                        responseObserver.onNext(resp);
                    }

                    if (Context.current().isCancelled()) {
                        LOG.info(
                                "Engine#{}: Connection broken detected, stop the engine",
                                config.getId());
                        runner.stop();
                    }
                } catch (InterruptedException e) {
                    LOG.error("Poll engine output channel fail. ", e);
                }
            }
        } catch (Throwable t) {
            LOG.error("Cdc engine failed.", t);
            try {
                runner.stop();
            } catch (Exception e) {
                LOG.warn("Failed to stop Engine#{}", config.getId(), e);
            }
        }
        LOG.info("End consuming events of table {}", config.getId());
    }
}
