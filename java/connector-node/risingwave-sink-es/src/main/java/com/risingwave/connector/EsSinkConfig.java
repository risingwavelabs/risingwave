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

public class EsSinkConfig extends CommonSinkConfig {
    /** Required */
    private String url;

    /** Required */
    private String index;

    /** Optional, delimeter for generating id */
    private String delimeter;

    @JsonCreator
    public EsSinkConfig(
            @JsonProperty(value = "url") String url,
            @JsonProperty(value = "index") String index,
            @JsonProperty(value = "delimeter") String delimeter) {
        this.url = url;
        this.index = index;
        this.delimeter = delimeter == null ? "_" : delimeter;
    }

    public String getUrl() {
        return url;
    }

    public String getIndex() {
        return index;
    }

    public String getDelimeter() {
        return delimeter;
    }
}
