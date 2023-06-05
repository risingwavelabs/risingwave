// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.risingwave.connector.api;

import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.Data;
import com.risingwave.proto.Data.DataType.TypeName;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TableSchema {
    private final List<String> columnNames;
    private final Map<String, TypeName> columns;
    private final Map<String, Integer> columnIndices;
    private List<ColumnDesc> columnDescs;

    private final List<String> primaryKeys;

    public TableSchema(
            List<String> columnNames, List<Data.DataType> dataTypes, List<String> primaryKeys) {
        this.columnNames = columnNames;
        this.primaryKeys = primaryKeys;
        this.columns = new HashMap<>();
        this.columnIndices = new HashMap<>();
        this.columnDescs = new ArrayList<>();
        for (int i = 0; i < columnNames.size(); i++) {
            columns.put(columnNames.get(i), dataTypes.get(i).getTypeName());
            columnIndices.put(columnNames.get(i), i);
            columnDescs.add(new ColumnDesc(columnNames.get(i), dataTypes.get(i)));
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

    public ColumnDesc getColumnDesc(int index) {
        return columnDescs.get(index);
    }

    public Map<String, TypeName> getColumnTypes() {
        return new HashMap<>(columns);
    }

    public String[] getColumnNames() {
        return columnNames.toArray(new String[0]);
    }

    public List<ColumnDesc> getColumnDescs() {
        return columnDescs;
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
