package com.risingwave.java.binding;

public class Record implements AutoCloseable {
    final long pointer;
    boolean isClosed;

    Record(long pointer) {
        this.pointer = pointer;
        this.isClosed = false;
    }

    public byte[] getKey() {
        return Binding.rowGetKey(pointer);
    }

    public boolean isNull(int index) {
        return Binding.rowIsNull(pointer, index);
    }

    public long getLong(int index) {
        return Binding.rowGetInt64Value(pointer, index);
    }

    public String getString(int index) {
        return Binding.rowGetStringValue(pointer, index);
    }

    @Override
    public void close() {
        if (!isClosed) {
            isClosed = true;
            Binding.rowClose(pointer);
        }
    }
}
