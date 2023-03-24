package com.risingwave.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.risingwave.connector.api.sink.CommonSinkConfig;

public class JDBCSinkConfig extends CommonSinkConfig {
    private String jdbcUrl;

    private String tableName;

    @JsonCreator
    public JDBCSinkConfig(
            @JsonProperty(value = "jdbc.url") String jdbcUrl,
            @JsonProperty(value = "table.name") String tableName) {
        this.jdbcUrl = jdbcUrl;
        this.tableName = tableName;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
