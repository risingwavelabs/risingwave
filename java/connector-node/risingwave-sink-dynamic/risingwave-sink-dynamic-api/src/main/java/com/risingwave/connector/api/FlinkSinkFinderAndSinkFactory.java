package com.risingwave.connector.api;

import org.apache.flink.table.factories.DynamicTableSinkFactory;

/**
 * Different sinks need to implement this method to provide the required
 * `FlinkSinkTableSchemaFinder` and `DynamicTableSinkFactory` for flink sinks
 */
public interface FlinkSinkFinderAndSinkFactory {
    public FlinkSinkTableSchemaFinder getFlinkSinkTableSchemaFinder(
            TableSchema tableSchema, FlinkDynamicAdapterConfig config);

    public DynamicTableSinkFactory getDynamicTableSinkFactory();
}
