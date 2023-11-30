package com.risingwave.connector.context;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

/**
 * It is used when flink creating a sink, which should contain the serialization method
 * corresponding to the datatypes of the flink. But the current sink doesn't use it, so it just
 * throws an exception here.
 */
public class DynamicTableSinkContextImpl implements DynamicTableSink.Context {
    @Override
    public boolean isBounded() {
        return false;
    }

    @Override
    public <T> TypeInformation<T> createTypeInformation(DataType consumedDataType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> TypeInformation<T> createTypeInformation(LogicalType consumedLogicalType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DynamicTableSink.DataStructureConverter createDataStructureConverter(
            DataType consumedDataType) {
        throw new UnsupportedOperationException();
    }
}
