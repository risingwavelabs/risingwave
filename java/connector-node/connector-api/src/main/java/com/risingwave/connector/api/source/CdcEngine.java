package com.risingwave.connector.api.source;

import com.risingwave.proto.ConnectorServiceProto;
import java.util.concurrent.BlockingQueue;

public interface CdcEngine extends Runnable {
    long getId();

    void stop() throws Exception;

    BlockingQueue<ConnectorServiceProto.GetEventStreamResponse> getOutputChannel();
}
