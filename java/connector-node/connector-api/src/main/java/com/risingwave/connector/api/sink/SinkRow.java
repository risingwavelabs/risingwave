package com.risingwave.connector.api.sink;

import com.risingwave.proto.Data;

public interface SinkRow {
    public Object get(int index);

    public Data.Op getOp();

    public int size();
}
