package com.risingwave.connector;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.risingwave.connector.common.S3Config;

public class HudiSinkConfig extends S3Config {
    @JsonProperty(value = "base.path")
    private String basePath;

    @JsonProperty(value = "table.name")
    private String tableName;

    public String getBasePath() {
        return basePath;
    }

    public String getTableName() {
        return tableName;
    }
}
