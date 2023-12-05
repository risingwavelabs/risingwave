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

package com.risingwave.mock.flink.runtime.context;

import java.util.Optional;
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

    @Override
    public Optional<int[][]> getTargetColumns() {
        throw new UnsupportedOperationException();
    }
}
