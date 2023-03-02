package com.risingwave.connector;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkBase;
import com.risingwave.connector.api.sink.SinkFactory;
import java.util.Map;

public class PrintSinkFactory implements SinkFactory {

    @Override
    public SinkBase create(TableSchema tableSchema, Map<String, String> tableProperties) {
        return new PrintSink(tableProperties, tableSchema);
    }
}
