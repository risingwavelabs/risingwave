package com.risingwave.connector;

import com.getindata.connectors.http.internal.table.sink.HttpDynamicTableSinkFactory;
import com.risingwave.connector.api.FlinkDynamicAdapterConfig;
import com.risingwave.connector.api.FlinkSinkFinderAndSinkFactory;
import com.risingwave.connector.api.FlinkSinkTableSchemaFinder;
import com.risingwave.connector.api.TableSchema;
import org.apache.flink.table.factories.DynamicTableSinkFactory;

/**
 * The `FlinkSinkFinderAndSinkFactory` implementation of the http sink is responsible for creating
 * the http counterpart of the `FlinkSinkTableSchemaFinder` and `DynamicTableSinkFactory`
 */
public class HttpFlinkSinkFinderAndSinkFactory implements FlinkSinkFinderAndSinkFactory {
    @Override
    public FlinkSinkTableSchemaFinder getFlinkSinkTableSchemaFinder(
            TableSchema tableSchema, FlinkDynamicAdapterConfig config) {
        return new HttpFlinkSinkTableSchemaFinder();
    }

    @Override
    public DynamicTableSinkFactory getDynamicTableSinkFactory() {
        return new HttpDynamicTableSinkFactory();
    }
}
