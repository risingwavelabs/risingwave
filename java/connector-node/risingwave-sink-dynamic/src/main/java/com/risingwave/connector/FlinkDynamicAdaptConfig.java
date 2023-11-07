package com.risingwave.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.risingwave.connector.api.sink.CommonSinkConfig;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.ObjectPath;

public class FlinkDynamicAdaptConfig extends CommonSinkConfig {
    Map<String, String> option;

    @JsonCreator
    public FlinkDynamicAdaptConfig(Map<String, String> tableProperties) {
        super(
                tableProperties.get("connector"),
                Boolean.valueOf(tableProperties.get("force_append_only")),
                tableProperties.get("primary_key"));
        this.option = tableProperties;
    }

    public ObjectPath getTablePath() {
        throw new RuntimeException("Cannot get table with connector type " + getConnector());
    }

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
