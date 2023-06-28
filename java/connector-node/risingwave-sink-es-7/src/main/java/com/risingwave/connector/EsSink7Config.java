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

public class EsSink7Config extends CommonSinkConfig {
    /** Required */
    private String url;

    /** Required */
    private String index;

    /** Optional, delimiter for generating id */
    private String delimiter;

    private String username;

    private String password;

    @JsonCreator
    public EsSink7Config(
            @JsonProperty(value = "url", required = true) String url,
            @JsonProperty(value = "index", required = true) String index,
            @JsonProperty(value = "delimiter") String delimiter,
            @JsonProperty(value = "username") String username,
            @JsonProperty(value = "password") String password) {
        this.url = url;
        this.index = index;
        this.delimiter = delimiter == null ? "_" : delimiter;
        this.username = username;
        this.password = password;
    }

    public String getUrl() {
        return url;
    }

    public String getIndex() {
        return index;
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
}
