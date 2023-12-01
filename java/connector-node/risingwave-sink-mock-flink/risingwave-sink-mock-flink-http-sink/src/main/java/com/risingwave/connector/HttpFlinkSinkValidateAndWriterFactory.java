package com.risingwave.connector;

import com.getindata.connectors.http.internal.table.sink.HttpDynamicTableSinkFactory;
import com.risingwave.connector.api.FlinkDynamicAdapterConfig;
import com.risingwave.connector.api.FlinkSinkValidateAndWriterFactory;
import com.risingwave.connector.api.TableSchema;
import io.grpc.StatusRuntimeException;
import org.apache.flink.table.factories.DynamicTableSinkFactory;

/**
 * The `FlinkSinkFinderAndSinkFactory` implementation of the http sink is responsible for creating
 * the http counterpart of the `FlinkSinkTableSchemaFinder` and `DynamicTableSinkFactory`
 */
public class HttpFlinkSinkValidateAndWriterFactory implements FlinkSinkValidateAndWriterFactory {
    @Override
    public void validate(TableSchema tableSchema, FlinkDynamicAdapterConfig config)
            throws StatusRuntimeException {}

    @Override
    public DynamicTableSinkFactory getDynamicTableSinkFactory() {
        return new HttpDynamicTableSinkFactory();
    }
}
