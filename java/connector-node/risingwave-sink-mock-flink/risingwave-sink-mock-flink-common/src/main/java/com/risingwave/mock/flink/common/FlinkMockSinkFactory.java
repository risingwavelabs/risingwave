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

package com.risingwave.mock.flink.common;

import com.risingwave.connector.api.TableSchema;
import io.grpc.StatusRuntimeException;
import org.apache.flink.table.factories.DynamicTableSinkFactory;

/**
 * Different sinks need to implement this method to provide the required `validate` and
 * `DynamicTableSinkFactory` for flink sinks
 */
public interface FlinkMockSinkFactory {
    /**
     * It is responsible for validating our schema, the default use of the flink catalog interface.
     * But some sinks do not implement the catalog, we need to implement their own checksums
     */
    public void validate(TableSchema tableSchema, FlinkDynamicAdapterConfig config)
            throws StatusRuntimeException;

    public DynamicTableSinkFactory getDynamicTableSinkFactory();
}
