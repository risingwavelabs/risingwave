package com.risingwave.connector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkFactory;
import com.risingwave.connector.api.sink.SinkWriter;
import com.risingwave.connector.api.sink.SinkWriterV1;
import com.risingwave.proto.Catalog;
import java.util.*;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.connector.sink.*;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;

public class FlinkDynamicAdaptFactory implements SinkFactory {

    public DynamicTableSink.SinkRuntimeProvider buildDynamicTableSinkProvider(
            TableSchema tableSchema, FlinkDynamicAdaptConfig config) {
        List<Column> flinkColumns = FlinkDynamicAdaptUtil.getFlinkColumnsFromSchema(tableSchema);

        // Start with the default value, and add as needed later
        ObjectIdentifier objectIdentifier =
                ObjectIdentifier.of("catalog_name", "database_name", "obj_name");
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        DynamicTableSinkFactory dynamicTableSinkFactory =
                FactoryUtil.discoverFactory(
                        contextClassLoader, DynamicTableSinkFactory.class, config.getConnector());
        Set<ConfigOption<?>> configOptions = new HashSet<>();
        configOptions.addAll(dynamicTableSinkFactory.requiredOptions());
        configOptions.addAll(dynamicTableSinkFactory.optionalOptions());
        config.processOption(configOptions);

        Schema.Builder schemaBuilder = Schema.newBuilder();
        tableSchema
                .getColumnDescs()
                .forEach(
                        (columnDesc) ->
                                schemaBuilder.column(
                                        columnDesc.getName(), columnDesc.getDataType().toString()));
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
        DynamicTableSink.Context context = new SinkRuntimeProviderContext(false);
        // Start with the default value, and add as needed later
        return dynamicTableSinkFactory
                .createDynamicTableSink(defaultDynamicTableContext)
                .getSinkRuntimeProvider(context);
    }

    @Override
    public SinkWriter createWriter(TableSchema tableSchema, Map<String, String> tableProperties) {
        ObjectMapper mapper = new ObjectMapper();
        FlinkDynamicAdaptConfig flinkDynamicAdaptConfig =
                mapper.convertValue(tableProperties, FlinkDynamicAdaptConfig.class);

        DynamicTableSink.SinkRuntimeProvider sinkRuntimeProvider =
                buildDynamicTableSinkProvider(tableSchema, flinkDynamicAdaptConfig);
        if (sinkRuntimeProvider instanceof SinkProvider) {
            Sink<RowData, ?, ?, ?> sink = ((SinkProvider) sinkRuntimeProvider).createSink();
            return new SinkWriterV1.Adapter(new FlinkDynamicAdaptSink<>(tableSchema, sink));
        } else if (sinkRuntimeProvider instanceof SinkFunctionProvider) {
            // need test
            SinkFunction<RowData> sinkFunction =
                    ((SinkFunctionProvider) sinkRuntimeProvider).createSinkFunction();
            return new SinkWriterV1.Adapter(new FlinkDynamicAdaptSink<>(tableSchema, sinkFunction));
        } else if (sinkRuntimeProvider instanceof OutputFormatProvider) {
            // need test
            OutputFormat<RowData> outputFormat =
                    ((OutputFormatProvider) sinkRuntimeProvider).createOutputFormat();
            SinkFunction<RowData> sinkFunction = new OutputFormatSinkFunction<>(outputFormat);
            return new SinkWriterV1.Adapter(new FlinkDynamicAdaptSink<>(tableSchema, sinkFunction));
        } else if (sinkRuntimeProvider instanceof SinkV2Provider) {
            org.apache.flink.api.connector.sink2.Sink<RowData> sink =
                    ((SinkV2Provider) sinkRuntimeProvider).createSink();
            return new SinkWriterV1.Adapter(new FlinkDynamicAdaptSink<>(tableSchema, sink));
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
        FlinkDynamicAdaptConfig flinkDynamicAdaptConfig =
                mapper.convertValue(tableProperties, FlinkDynamicAdaptConfig.class);
        FlinkDynamicAdaptUtil.discoverSchemaFinder(tableSchema, flinkDynamicAdaptConfig)
                .validate(tableSchema.getColumnDescs());
    }
}
