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

package com.risingwave.mock.flink.http;

import com.getindata.connectors.http.internal.table.sink.HttpDynamicTableSinkFactory;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.mock.flink.common.FlinkDynamicAdapterConfig;
import com.risingwave.mock.flink.common.FlinkMockSinkFactory;
import io.grpc.StatusRuntimeException;
import org.apache.flink.table.factories.DynamicTableSinkFactory;

/**
 * The `FlinkMockSinkFactory` implementation of the http sink is responsible for creating the http
 * counterpart of the `DynamicTableSinkFactory`. And `validate` don't need to do anything.
 */
public class HttpFlinkMockSinkFactory implements FlinkMockSinkFactory {
    @Override
    public void validate(TableSchema tableSchema, FlinkDynamicAdapterConfig config)
            throws StatusRuntimeException {}

    @Override
    public DynamicTableSinkFactory getDynamicTableSinkFactory() {
        return new HttpDynamicTableSinkFactory();
    }
}
