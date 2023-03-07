package com.risingwave.connector.api.sink;

import com.risingwave.proto.Data;

public class ArraySinkrow implements SinkRow {
    public final Object[] values;
    public final Data.Op op;

    public ArraySinkrow(Data.Op op, Object... value) {
        this.op = op;
        this.values = value;
    }

    @Override
    public Object get(int index) {
        return values[index];
    }

    @Override
    public Data.Op getOp() {
        return op;
    }

    @Override
    public int size() {
        return values.length;
    }
}
