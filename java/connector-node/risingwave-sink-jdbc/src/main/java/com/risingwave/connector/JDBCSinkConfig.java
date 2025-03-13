/*
 * Copyright 2025 RisingWave Labs
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

    @JsonProperty private String user;

    @JsonProperty private String password;

    private String tableName;

    private String sinkType;

    private final boolean isUpsertSink;

    @JsonProperty(value = "schema.name")
    private String schemaName;

    @JsonProperty(value = "jdbc.query.timeout")
    private int queryTimeoutSeconds = 60;

    @JsonProperty(value = "jdbc.auto.commit")
    private boolean autoCommit = false;

    @JsonCreator
    public JDBCSinkConfig(
            @JsonProperty(value = "jdbc.url") String jdbcUrl,
            @JsonProperty(value = "table.name") String tableName,
            @JsonProperty(value = "type") String sinkType) {
        this.jdbcUrl = jdbcUrl;
        this.tableName = tableName;
        this.sinkType = sinkType;
        this.isUpsertSink = "upsert".equalsIgnoreCase(sinkType);
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSinkType() {
        return sinkType;
    }

    public boolean isUpsertSink() {
        return this.isUpsertSink;
    }

    public int getQueryTimeout() {
        return queryTimeoutSeconds;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }
}
