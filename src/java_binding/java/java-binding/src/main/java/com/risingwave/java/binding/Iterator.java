package com.risingwave.java.binding;

import com.risingwave.proto.JavaBinding.ReadPlan;

public class Iterator implements AutoCloseable {
    private final long pointer;
    private boolean isClosed;

    public Iterator(ReadPlan readPlan) {
        this.pointer = Binding.iteratorNew(readPlan.toByteArray());
        this.isClosed = false;
    }

    public KeyedRow next() {
        long pointer = Binding.iteratorNext(this.pointer);
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
