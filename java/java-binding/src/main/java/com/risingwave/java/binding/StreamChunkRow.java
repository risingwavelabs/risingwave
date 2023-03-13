package com.risingwave.java.binding;

import com.risingwave.proto.Data;

public class StreamChunkRow extends BaseRow {
    public StreamChunkRow(long pointer) {
        super(pointer);
    }

    public Data.Op getOp() {
        return Data.Op.forNumber(Binding.rowGetOp(pointer));
    }
}
