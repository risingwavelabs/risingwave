package com.risingwave.connector.api;

import com.google.common.collect.Lists;
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.Data.DataType.TypeName;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TableSchema {
    private final List<String> columnNames;
    private final Map<String, TypeName> columns;
    private final Map<String, Integer> columnIndices;

    private final List<String> primaryKeys;

    public TableSchema(
            List<String> columnNames, List<TypeName> typeNames, List<String> primaryKeys) {
        this.columnNames = columnNames;
        this.primaryKeys = primaryKeys;
        this.columns = new HashMap<>();
        this.columnIndices = new HashMap<>();
        for (int i = 0; i < columnNames.size(); i++) {
            columns.put(columnNames.get(i), typeNames.get(i));
            columnIndices.put(columnNames.get(i), i);
        }
    }

    public int getNumColumns() {
        return columns.size();
    }

    public int getColumnIndex(String columnName) {
        return columnIndices.get(columnName);
    }

    public TypeName getColumnType(String columnName) {
        return columns.get(columnName);
    }

    public Map<String, TypeName> getColumnTypes() {
        return new HashMap<>(columns);
    }

    public String[] getColumnNames() {
        return columnNames.toArray(new String[0]);
    }

    public static TableSchema getMockTableSchema() {
        return new TableSchema(
                Lists.newArrayList("id", "name"),
                Lists.newArrayList(TypeName.INT32, TypeName.VARCHAR),
                Lists.newArrayList("id"));
    }

    public static ConnectorServiceProto.TableSchema getMockTableProto() {
        return ConnectorServiceProto.TableSchema.newBuilder()
                .addColumns(
                        ConnectorServiceProto.TableSchema.Column.newBuilder()
                                .setName("id")
                                .setDataType(TypeName.INT32)
                                .build())
                .addColumns(
                        ConnectorServiceProto.TableSchema.Column.newBuilder()
                                .setName("name")
                                .setDataType(TypeName.VARCHAR)
                                .build())
                .addAllPkIndices(List.of(1))
                .build();
    }

    public Object getFromRow(String columnName, SinkRow row) {
        return row.get(columnIndices.get(columnName));
    }

    public static TableSchema fromProto(ConnectorServiceProto.TableSchema tableSchema) {
        return new TableSchema(
                tableSchema.getColumnsList().stream()
                        .map(ConnectorServiceProto.TableSchema.Column::getName)
                        .collect(Collectors.toList()),
                tableSchema.getColumnsList().stream()
                        .map(ConnectorServiceProto.TableSchema.Column::getDataType)
                        .collect(Collectors.toList()),
                tableSchema.getPkIndicesList().stream()
                        .map(i -> tableSchema.getColumns(i).getName())
                        .collect(Collectors.toList()));
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    @Override
    public String toString() {
        return "TableSchema{"
                + "columnNames="
                + columnNames
                + ", columns="
                + columns
                + ", columnIndices="
                + columnIndices
                + ", primaryKeys="
                + primaryKeys
                + '}';
    }
}
