package com.risingwave.java.binding;

import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.JavaBinding.ReadPlan;

public class StreamChunkIterator implements AutoCloseable {
    private final long pointer;
    private boolean isClosed;

    public StreamChunkIterator(byte[] data) {
        this.pointer = Binding.streamChunkIteratorNew(data);
        this.isClosed = false;
    }

    public KeyedRow next() {
        long pointer = Binding.streamChunkIteratorNext(this.pointer);
        if (pointer == 0) {
            return null;
        }
        return new KeyedRow(pointer);
    }

    @Override
    public void close() {
        if (!isClosed) {
            isClosed = true;
            Binding.iteratorClose(pointer);
        }
    }
}
