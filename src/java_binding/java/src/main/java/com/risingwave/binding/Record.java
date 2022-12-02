package com.risingwave.binding;

public class Record implements AutoCloseable {
    final long pointer;
    boolean isClosed;

    Record(long pointer) {
        this.pointer = pointer;
        this.isClosed = false;
    }

    byte[] getKey() {
        return Binding.recordGetKey(pointer);
    }

    byte[] getValue() {
        return Binding.recordGetValue(pointer);
    }

    @Override
    public void close() {
        if (!isClosed) {
            isClosed = true;
            Binding.recordClose(pointer);
        }
    }
}
