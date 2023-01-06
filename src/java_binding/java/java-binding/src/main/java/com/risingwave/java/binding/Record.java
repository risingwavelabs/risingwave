package com.risingwave.java.binding;

public class Record implements AutoCloseable {
    final long pointer;
    boolean isClosed;

    Record(long pointer) {
        this.pointer = pointer;
        this.isClosed = false;
    }

    public byte[] getKey() {
        return Binding.recordGetKey(pointer);
    }

    public boolean isNull(int index) {
        return Binding.recordIsNull(pointer, index);
    }

    public long getLong(int index) {
        return Binding.recordGetInt64Value(pointer, index);
    }

    public String getString(int index) {
        return Binding.recordGetStringValue(pointer, index);
    }

    @Override
    public void close() {
        if (!isClosed) {
            isClosed = true;
            Binding.recordClose(pointer);
        }
    }
}
