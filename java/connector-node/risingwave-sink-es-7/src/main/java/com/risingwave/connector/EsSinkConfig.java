/*
 * Copyright 2024 RisingWave Labs
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

public class EsSinkConfig extends CommonSinkConfig {
    /** Required */
    private String url;

    /** Optional */
    @JsonProperty(value = "index")
    private String index;

    /** Optional, delimiter for generating id */
    @JsonProperty(value = "delimiter")
    private String delimiter;

    @JsonProperty(value = "username")
    private String username;

    @JsonProperty(value = "password")
    private String password;

    @JsonProperty(value = "index_column")
    private String indexColumn;

    @JsonProperty(value = "max_task_num")
    private Integer maxTaskNum;

    @JsonCreator
    public EsSinkConfig(@JsonProperty(value = "url") String url) {
        this.url = url;
    }

    public String getUrl() {
        return url;
    }

    public String getIndex() {
        return index;
    }

    public EsSinkConfig withIndex(String index) {
        this.index = index;
        return this;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public EsSinkConfig withDelimiter(String delimiter) {
        this.delimiter = delimiter;
        return this;
    }

    public EsSinkConfig withUsername(String username) {
        this.username = username;
        return this;
    }

    public EsSinkConfig withPassword(String password) {
        this.password = password;
        return this;
    }

    public String getIndexColumn() {
        return indexColumn;
    }

    public EsSinkConfig withIndexColumn(String indexColumn) {
        this.indexColumn = indexColumn;
        return this;
    }

    public Integer getMaxTaskNum() {
        return maxTaskNum;
    }

    public EsSinkConfig withMaxTaskNum(Integer maxTaskNum) {
        this.maxTaskNum = maxTaskNum;
        return this;
    }
}
