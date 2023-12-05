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

package com.risingwave.mock.flink.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.risingwave.connector.api.sink.CommonSinkConfig;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.ObjectPath;

/*
 * The user-input configuration items will be passed downstream to create the corresponding factory.
 */
public class FlinkDynamicAdapterConfig extends CommonSinkConfig {
    Map<String, String> option;

    @JsonCreator
    public FlinkDynamicAdapterConfig(Map<String, String> tableProperties) {
        super(
                tableProperties.get("connector"),
                Boolean.valueOf(tableProperties.get("force_append_only")),
                tableProperties.get("primary_key"));
        this.option = tableProperties;
    }

    public ObjectPath getTablePath() {
        throw new RuntimeException("Cannot get table with connector type " + getConnector());
    }

    /**
     * Our option contains both kinds of parameters, and it is the responsibility of the correct
     * parameter to be chosen here
     *
     * @param needOptionSet: Need option names
     */
    public void processOption(Set<ConfigOption<?>> needOptionSet) {
        Set<String> needOptionStringSet =
                needOptionSet.stream().map(c -> c.key()).collect(Collectors.toSet());
        option =
                option.entrySet().stream()
                        .filter(entry -> needOptionStringSet.contains(entry.getKey()))
                        .collect(Collectors.toMap(a -> a.getKey(), a -> a.getValue()));
    }

    public Map<String, String> getOption() {
        return option;
    }
}
