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

public class CassandraConfig extends CommonSinkConfig {
    /** Required */
    private String type;

    /** Required */
    private String url;

    /** Required */
    private String keyspace;

    /** Required */
    private String table;

    /** Required */
    private String datacenter;

    @JsonProperty(value = "cassandra.username")
    private String username;

    @JsonProperty(value = "cassandra.password")
    private String password;

    @JsonProperty(value = "cassandra.max_batch_rows")
    private Integer maxBatchRows = 512;

    @JsonProperty(value = "cassandra.request_timeout_ms")
    private Integer requestTimeoutMs = 2000;

    @JsonCreator
    public CassandraConfig(
            @JsonProperty(value = "cassandra.url") String url,
            @JsonProperty(value = "cassandra.keyspace") String keyspace,
            @JsonProperty(value = "cassandra.table") String table,
            @JsonProperty(value = "cassandra.datacenter") String datacenter,
            @JsonProperty(value = "type") String type) {
        this.url = url;
        this.keyspace = keyspace;
        this.table = CassandraUtil.convertCQLIdentifiers(table);
        this.datacenter = datacenter;
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public String getUrl() {
        return url;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public String getTable() {
        return table;
    }

    public String getDatacenter() {
        return datacenter;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public CassandraConfig withUsername(String username) {
        this.username = username;
        return this;
    }

    public CassandraConfig withPassword(String password) {
        this.password = password;
        return this;
    }

    public Integer getMaxBatchRows() {
        return maxBatchRows;
    }

    public CassandraConfig withMaxBatchRows(Integer maxBatchRows) {
        if (maxBatchRows > 65536 || maxBatchRows < 1) {
            throw new IllegalArgumentException(
                    "Cassandra sink option: maxBatchRows must be <= 65535 and >= 1");
        }
        this.maxBatchRows = maxBatchRows;
        return this;
    }

    public Integer getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    public CassandraConfig withRequestTimeoutMs(Integer requestTimeoutMs) {
        if (requestTimeoutMs < 1) {
            throw new IllegalArgumentException(
                    "Cassandra sink option: requestTimeoutMs must be >= 1");
        }
        this.requestTimeoutMs = requestTimeoutMs;
        return this;
    }
}
