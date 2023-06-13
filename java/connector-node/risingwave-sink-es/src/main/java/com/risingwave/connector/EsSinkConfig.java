package com.risingwave.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.risingwave.connector.api.sink.CommonSinkConfig;
public class EsSinkConfig extends CommonSinkConfig {
    private String esUrl;
    private String index;

    public EsSinkConfig(
            @JsonProperty(value = "es.url") String esUrl,
            @JsonProperty(value = "es.index") String index) {
        this.esUrl = esUrl;
        this.index = index;
    }

    public String getEsUrl() { return esUrl; }
    public String getIndex() { return index; }
}
