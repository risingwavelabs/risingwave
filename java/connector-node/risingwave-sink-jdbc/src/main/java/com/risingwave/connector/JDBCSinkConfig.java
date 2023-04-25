/*
 * Copyright 2023 RisingWave Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.risingwave.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.risingwave.connector.api.sink.CommonSinkConfig;

public class JDBCSinkConfig extends CommonSinkConfig {
    private String jdbcUrl;

    private String tableName;

    private String sinkType;

    @JsonProperty(value = "force_append_only")
    private Boolean forceAppendOnly;

    @JsonCreator
    public JDBCSinkConfig(
            @JsonProperty(value = "jdbc.url") String jdbcUrl,
            @JsonProperty(value = "table.name") String tableName,
            @JsonProperty(value = "type") String sinkType) {
        this.jdbcUrl = jdbcUrl;
        this.tableName = tableName;
        this.sinkType = sinkType;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSinkType() {
        return sinkType;
    }
}
