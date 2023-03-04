package com.risingwave.connector.api.sink;

import com.risingwave.connector.api.TableSchema;

public abstract class SinkBase implements Sink {
    TableSchema tableSchema;

    public SinkBase(TableSchema tableSchema) {
        this.tableSchema = tableSchema;
    }

    public TableSchema getTableSchema() {
        return tableSchema;
    }
}
