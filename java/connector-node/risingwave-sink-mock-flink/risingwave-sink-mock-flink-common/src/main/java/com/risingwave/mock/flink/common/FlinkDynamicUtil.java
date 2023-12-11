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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

public class FlinkDynamicUtil {
    private static final String TIMESTAMP_LTZ_FORMAT = "TIMESTAMP\\(\\d+\\) WITH LOCAL TIME ZONE";

    public static DataType getCorrespondingFlinkType(Data.DataType dataType) {
        switch (dataType.getTypeName()) {
            case INT16:
                return DataTypes.SMALLINT();
            case INT32:
                return DataTypes.INT();
            case INT64:
                return DataTypes.BIGINT();
            case FLOAT:
                return DataTypes.FLOAT();
            case DOUBLE:
                return DataTypes.DOUBLE();
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case VARCHAR:
                return DataTypes.STRING();
            case DECIMAL:
                // Because there is no 'scale' and 'precision' in 'DECIMAL' within RW,
                // this is just a simulation here, which may be incompatible with some interfaces.
                return DataTypes.DECIMAL(9, 1);
            case TIMESTAMP:
                // Like DECIMAL
                return DataTypes.TIMESTAMP();
            case TIMESTAMPTZ:
                // Like DECIMAL
                return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE();
            case DATE:
                return DataTypes.DATE();
            case TIME:
                return DataTypes.TIME();
            case BYTEA:
                return DataTypes.BYTES();
            case LIST:
            case STRUCT:
            case INTERVAL:
                throw Status.UNIMPLEMENTED
                        .withDescription(String.format("not support %s now", dataType))
                        .asRuntimeException();
            default:
                throw Status.INVALID_ARGUMENT
                        .withDescription("unspecified type" + dataType)
                        .asRuntimeException();
        }
    }

    /** Converting RW's schema to Flink's schema */
    public static List<Column> getFlinkColumnsFromSchema(TableSchema tableSchema) {
        List<Column> columns = new ArrayList<>();
        for (ColumnDesc columnDesc : tableSchema.getColumnDescs()) {
            columns.add(
                    Column.physical(
                            columnDesc.getName(),
                            getCorrespondingFlinkType(columnDesc.getDataType())));
        }
        return columns;
    }

    public static void validateSchemaWithCatalog(
            TableSchema tableSchema,
            FlinkDynamicAdapterConfig config,
            CatalogFactory catalogFactory)
            throws StatusRuntimeException {
        List<Column> flinkColumns = getColumns(tableSchema, config, catalogFactory);
        List<ColumnDesc> rwColumnDescs = tableSchema.getColumnDescs();
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

    private static List<Column> getColumns(
            TableSchema tableSchema,
            FlinkDynamicAdapterConfig config,
            CatalogFactory catalogFactory) {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        Configuration configuration = new Configuration();
        Set<ConfigOption<?>> configOptions = catalogFactory.requiredOptions();
        configOptions.addAll(catalogFactory.optionalOptions());
        config.processOption(configOptions);

        Schema.Builder schemaBuilder = Schema.newBuilder();
        tableSchema
                .getColumnDescs()
                .forEach(
                        (columnDesc) ->
                                schemaBuilder.column(
                                        columnDesc.getName(),
                                        getCorrespondingFlinkType(columnDesc.getDataType())));
        // Start with the default value, and add as needed later
        FactoryUtil.DefaultCatalogContext defaultCatalogContext =
                new FactoryUtil.DefaultCatalogContext(
                        "catalog", config.getOption(), configuration, contextClassLoader);
        Catalog catalog = catalogFactory.createCatalog(defaultCatalogContext);

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
                Pattern regex = Pattern.compile(TIMESTAMP_LTZ_FORMAT);
                Matcher matcher = regex.matcher(flinkType.toString());
                return matcher.matches();
            default:
                return getCorrespondingFlinkType(dataType).equals(flinkType);
        }
    }
}
