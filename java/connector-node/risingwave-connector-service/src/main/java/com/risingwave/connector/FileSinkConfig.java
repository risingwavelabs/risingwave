package com.risingwave.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.risingwave.connector.api.sink.CommonSinkConfig;

public class FileSinkConfig extends CommonSinkConfig {
    private String sinkPath;

    @JsonCreator
    public FileSinkConfig(@JsonProperty(value = "output.path") String sinkPath) {
        this.sinkPath = sinkPath;
    }

    public String getSinkPath() {
        return sinkPath;
    }

    public void setSinkPath(String sinkPath) {
        this.sinkPath = sinkPath;
    }
}
