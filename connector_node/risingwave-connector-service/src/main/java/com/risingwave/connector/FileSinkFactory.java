package com.risingwave.connector;

import static io.grpc.Status.*;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkBase;
import com.risingwave.connector.api.sink.SinkFactory;
import java.util.Map;

public class FileSinkFactory implements SinkFactory {
    public static final String OUTPUT_PATH_PROP = "output.path";

    @Override
    public SinkBase create(TableSchema tableSchema, Map<String, String> tableProperties) {
        if (!tableProperties.containsKey(OUTPUT_PATH_PROP)) {
            throw INVALID_ARGUMENT
                    .withDescription(String.format("%s is not specified", OUTPUT_PATH_PROP))
                    .asRuntimeException();
        }
        String sinkPath = tableProperties.get(OUTPUT_PATH_PROP);
        return new FileSink(sinkPath, tableSchema);
    }
}
