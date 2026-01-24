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

package com.risingwave.connector.api.sink;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CommonSinkConfig {
    private String connector;

    @JsonProperty(value = "force_append_only")
    protected Boolean forceAppendOnly;

    @JsonProperty(value = "primary_key")
    protected String primaryKey;

    public CommonSinkConfig() {}

    public CommonSinkConfig(String connector, Boolean forceAppendOnly, String primaryKey) {
        this.connector = connector;
        this.forceAppendOnly = forceAppendOnly;
        this.primaryKey = primaryKey;
    }

    public String getConnector() {
        return connector;
    }

    public void setConnector(String connector) {
        this.connector = connector;
    }

    public Boolean getForceAppendOnly() {
        return forceAppendOnly;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setForceAppendOnly(Boolean forceAppendOnly) {
        this.forceAppendOnly = forceAppendOnly;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }
}
