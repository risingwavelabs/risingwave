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
import com.risingwave.connector.common.S3Config;

public class IcebergSinkConfig extends S3Config {
    private String sinkType;

    private String warehousePath;

    private String databaseName;

    private String tableName;

    @JsonProperty(value = "force_append_only")
    private Boolean forceAppendOnly;

    @JsonProperty(value = "primary_key")
    private String primaryKey;

    @JsonCreator
    public IcebergSinkConfig(
            @JsonProperty(value = "type") String sinkType,
            @JsonProperty(value = "warehouse.path") String warehousePath,
            @JsonProperty(value = "database.name") String databaseName,
            @JsonProperty(value = "table.name") String tableName) {
        this.sinkType = sinkType;
        this.warehousePath = warehousePath;
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public String getSinkType() {
        return sinkType;
    }

    public void setSinkType(String sinkType) {
        this.sinkType = sinkType;
    }

    public String getWarehousePath() {
        return warehousePath;
    }

    public void setWarehousePath(String warehousePath) {
        this.warehousePath = warehousePath;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
