package com.risingwave.connector.api;

import io.grpc.StatusRuntimeException;
import org.apache.flink.table.factories.DynamicTableSinkFactory;

/**
 * Different sinks need to implement this method to provide the required
 * `FlinkSinkTableSchemaFinder` and `DynamicTableSinkFactory` for flink sinks
 */
public interface FlinkSinkValidateAndWriterFactory {
    /**
     * It is responsible for validating our schema, the default use of the flink catalog interface.
     * But some sinks do not implement the catalog, we need to implement their own checksums
     */
    public void validate(TableSchema tableSchema, FlinkDynamicAdapterConfig config)
            throws StatusRuntimeException;

    public DynamicTableSinkFactory getDynamicTableSinkFactory();
}
