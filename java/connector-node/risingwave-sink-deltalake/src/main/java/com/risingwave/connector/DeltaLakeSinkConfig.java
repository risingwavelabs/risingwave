package com.risingwave.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.risingwave.connector.api.sink.CommonSinkConfig;

public class DeltaLakeSinkConfig extends CommonSinkConfig {
    private String location;

    private String locationType;

    @JsonCreator
    public DeltaLakeSinkConfig(
            @JsonProperty(value = "location") String location,
            @JsonProperty(value = "location.type") String locationType) {
        this.location = location;
        this.locationType = locationType;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getLocationType() {
        return locationType;
    }

    public void setLocationType(String locationType) {
        this.locationType = locationType;
    }
}
