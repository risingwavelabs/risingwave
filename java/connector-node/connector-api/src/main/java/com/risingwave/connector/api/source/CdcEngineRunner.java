package com.risingwave.connector.api.source;

public interface CdcEngineRunner {
    void start() throws Exception;

    void stop() throws Exception;

    CdcEngine getEngine();

    boolean isRunning();
}
