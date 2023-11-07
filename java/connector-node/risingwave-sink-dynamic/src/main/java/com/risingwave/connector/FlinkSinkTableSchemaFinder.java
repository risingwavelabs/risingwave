package com.risingwave.connector;

import com.risingwave.connector.api.ColumnDesc;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.proto.Data;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

public interface FlinkSinkTableSchemaFinder {
    public void validate(List<ColumnDesc> rwColumnDescs) throws StatusRuntimeException;
}

class DefaultTableSchemaFinder implements FlinkSinkTableSchemaFinder {
    Catalog catalog;
    FlinkDynamicAdaptConfig config;

    public DefaultTableSchemaFinder(TableSchema tableSchema, FlinkDynamicAdaptConfig config) {
        Configuration configuration = new Configuration();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        CatalogFactory catalogFactory =
                FactoryUtil.discoverFactory(
                        contextClassLoader, CatalogFactory.class, config.getConnector());
        Set<ConfigOption<?>> configOptions = catalogFactory.requiredOptions();
        configOptions.addAll(catalogFactory.optionalOptions());
        config.processOption(configOptions);

        Schema.Builder schemaBuilder = Schema.newBuilder();
        tableSchema
                .getColumnDescs()
                .forEach(
                        (columnDesc) ->
                                schemaBuilder.column(
                                        columnDesc.getName(), columnDesc.getDataType().toString()));
        // Start with the default value, and add as needed later
        FactoryUtil.DefaultCatalogContext defaultCatalogContext =
                new FactoryUtil.DefaultCatalogContext(
                        "catalog", config.getOption(), configuration, contextClassLoader);

        this.catalog = catalogFactory.createCatalog(defaultCatalogContext);
    }

    @Override
    public void validate(List<ColumnDesc> rwColumnDescs) throws StatusRuntimeException {
        List<Column> flinkColumns = getColumns();
        if (rwColumnDescs.size() != flinkColumns.size()) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("Don't match in the number of columns in the table")
                    .asRuntimeException();
        }

        Map<String, DataType> flinkColumnMap =
                flinkColumns.stream()
                        .collect(Collectors.toMap(e -> e.getName(), e -> e.getDataType()));
        for (ColumnDesc columnDesc : rwColumnDescs) {
            if (!flinkColumnMap.containsKey(columnDesc.getName())) {
                throw Status.FAILED_PRECONDITION
                        .withDescription(
                                String.format(
                                        "Don't match in the name, rw is %s", columnDesc.getName()))
                        .asRuntimeException();
            }
            if (!checkType(columnDesc.getDataType(), flinkColumnMap.get(columnDesc.getName()))) {
                throw Status.FAILED_PRECONDITION
                        .withDescription(
                                String.format(
                                        "Don't match in the type, name is %s, Sink is %s, rw is %s",
                                        columnDesc.getName(),
                                        flinkColumnMap.get(columnDesc.getName()),
                                        columnDesc.getDataType().getTypeName()))
                        .asRuntimeException();
            }
        }
    }

    private List<Column> getColumns() {
        List<Column> columns = new ArrayList<>();
        try {
            catalog.open();
            List<Schema.UnresolvedColumn> flinkColumns =
                    catalog.getTable(config.getTablePath()).getUnresolvedSchema().getColumns();
            catalog.close();
            for (int i = 0; i < flinkColumns.size(); i++) {
                Schema.UnresolvedColumn unresolvedColumn = flinkColumns.get(i);
                if (unresolvedColumn instanceof Schema.UnresolvedPhysicalColumn) {
                    Schema.UnresolvedPhysicalColumn c1 =
                            (Schema.UnresolvedPhysicalColumn) unresolvedColumn;
                    columns.add(Column.physical(c1.getName(), (DataType) (c1.getDataType())));
                } else {
                    throw new RuntimeException("Only support physical column");
                }
            }
            return columns;
        } catch (TableNotExistException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean checkType(Data.DataType dataType, DataType flinkType) {
        switch (dataType.getTypeName()) {
            case DECIMAL:
                return flinkType.toString().contains("DECIMAL");
            case TIMESTAMP:
                return flinkType.toString().contains("TIMESTAMP");
            case TIMESTAMPTZ:
                String pattern = "TIMESTAMP\\(\\d+\\) WITH TIME ZONE";
                Pattern regex = Pattern.compile(pattern);
                Matcher matcher = regex.matcher(flinkType.toString());
                return matcher.matches();
            default:
                return FlinkDynamicAdaptUtil.getCorrespondingFlinkType(dataType).equals(flinkType);
        }
    }
}

class HttpTableSchemaFinder implements FlinkSinkTableSchemaFinder {

    public HttpTableSchemaFinder(TableSchema tableSchema, FlinkDynamicAdaptConfig config) {
        return;
    }

    @Override
    public void validate(List<ColumnDesc> rwColumnDescs) throws StatusRuntimeException {
        // Don't need check schema
        return;
    }
}
