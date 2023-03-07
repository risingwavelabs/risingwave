package com.risingwave.connector.api.sink;

import com.risingwave.connector.api.TableSchema;
import java.util.Map;

public interface SinkFactory {
    SinkBase create(TableSchema tableSchema, Map<String, String> tableProperties);

    void validate(TableSchema tableSchema, Map<String, String> tableProperties);
}
