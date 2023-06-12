package com.risingwave.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.risingwave.connector.api.sink.CommonSinkConfig;
public class EsSinkConfig extends CommonSinkConfig {
    private String esUrl;

    public EsSinkConfig(
            @JsonProperty(value = "es.url") String esUrl) {
        this.esUrl = esUrl;
    }

    public String getEsUrl() { return esUrl; }
}
