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

package com.risingwave.mock.flink.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkFactory;
import com.risingwave.connector.api.sink.SinkWriter;
import com.risingwave.mock.flink.common.FlinkDynamicAdapterConfig;
import com.risingwave.mock.flink.common.FlinkDynamicUtil;
import com.risingwave.mock.flink.common.FlinkMockSinkFactory;
import com.risingwave.mock.flink.runtime.context.DynamicTableSinkContextImpl;
import com.risingwave.mock.flink.runtime.context.SinkWriterContextV2;
import com.risingwave.mock.flink.runtime.sinkwriter.AsyncSinkWriterImpl;
import com.risingwave.mock.flink.runtime.sinkwriter.SinkWriterImpl;
import com.risingwave.mock.flink.runtime.sinkwriter.SinkWriterV2Impl;
import com.risingwave.proto.Catalog;
import java.io.IOException;
import java.util.*;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.connector.sink.*;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

public class FlinkDynamicAdapterFactory implements SinkFactory {

    FlinkMockSinkFactory flinkMockSinkFactory;

    public FlinkDynamicAdapterFactory(FlinkMockSinkFactory flinkMockSinkFactory) {
        this.flinkMockSinkFactory = flinkMockSinkFactory;
    }

    // Create corresponding Sink according to different configurations.
    public DynamicTableSink.SinkRuntimeProvider buildDynamicTableSinkProvider(
            TableSchema tableSchema, FlinkDynamicAdapterConfig config) {
        List<Column> flinkColumns = FlinkDynamicUtil.getFlinkColumnsFromSchema(tableSchema);

        // Start with the default value, and add as needed later
        ObjectIdentifier objectIdentifier =
                ObjectIdentifier.of("catalog_name", "database_name", "obj_name");
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        DynamicTableSinkFactory dynamicTableSinkFactory =
                flinkMockSinkFactory.getDynamicTableSinkFactory();
        Set<ConfigOption<?>> configOptions = new HashSet<>();
        configOptions.addAll(
                dynamicTableSinkFactory.requiredOptions() == null
                        ? Collections.emptyList()
                        : dynamicTableSinkFactory.requiredOptions());
        configOptions.addAll(
                dynamicTableSinkFactory.optionalOptions() == null
                        ? Collections.emptyList()
                        : dynamicTableSinkFactory.optionalOptions());
        config.processOption(configOptions);

        Schema.Builder schemaBuilder = Schema.newBuilder();
        flinkColumns.forEach(
                (column) -> schemaBuilder.column(column.getName(), column.getDataType()));
        Schema shcema = schemaBuilder.primaryKey(tableSchema.getPrimaryKeys()).build();

        ResolvedCatalogTable resolvedCatalogTable =
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                shcema, null, tableSchema.getPrimaryKeys(), config.getOption()),
                        ResolvedSchema.of(flinkColumns));

        FactoryUtil.DefaultDynamicTableContext defaultDynamicTableContext =
                new FactoryUtil.DefaultDynamicTableContext(
                        objectIdentifier,
                        resolvedCatalogTable,
                        new HashMap<String, String>(),
                        new Configuration(),
                        contextClassLoader,
                        false);
        DynamicTableSink.Context context = new DynamicTableSinkContextImpl();
        // Start with the default value, and add as needed later
        return dynamicTableSinkFactory
                .createDynamicTableSink(defaultDynamicTableContext)
                .getSinkRuntimeProvider(context);
    }

    /**
     * According to different implementations, create different sink writer, and package it as a
     * RW-recognizable method(SinkWriterV1), after which the RW can call the method to complete
     * write directly.
     */
    @Override
    public SinkWriter createWriter(TableSchema tableSchema, Map<String, String> tableProperties) {
        ObjectMapper mapper = new ObjectMapper();
        FlinkDynamicAdapterConfig flinkDynamicAdapterConfig =
                mapper.convertValue(tableProperties, FlinkDynamicAdapterConfig.class);

        DynamicTableSink.SinkRuntimeProvider sinkRuntimeProvider =
                buildDynamicTableSinkProvider(tableSchema, flinkDynamicAdapterConfig);
        if (sinkRuntimeProvider instanceof SinkProvider) {
            Sink<RowData, ?, ?, ?> sink = ((SinkProvider) sinkRuntimeProvider).createSink();
            return new SinkWriterImpl<>(tableSchema, sink);
        } else if (sinkRuntimeProvider instanceof SinkV2Provider) {
            org.apache.flink.api.connector.sink2.Sink<RowData> sink =
                    ((SinkV2Provider) sinkRuntimeProvider).createSink();

            try {
                if (sink.createWriter(new SinkWriterContextV2()) instanceof AsyncSinkWriter) {
                    return new AsyncSinkWriterImpl(tableSchema, sink);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return new SinkWriterV2Impl(tableSchema, sink);
        } else {
            throw new TableException("Unsupported sink runtime provider.");
        }
    }

    @Override
    public void validate(
            TableSchema tableSchema,
            Map<String, String> tableProperties,
            Catalog.SinkType sinkType) {
        ObjectMapper mapper = new ObjectMapper();
        FlinkDynamicAdapterConfig flinkDynamicAdapterConfig =
                mapper.convertValue(tableProperties, FlinkDynamicAdapterConfig.class);
        flinkMockSinkFactory.validate(tableSchema, flinkDynamicAdapterConfig);
    }
}
