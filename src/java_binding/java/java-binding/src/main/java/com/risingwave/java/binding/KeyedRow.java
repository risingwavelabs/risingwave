package com.risingwave.java.binding;

public class KeyedRow implements AutoCloseable {
    private final long pointer;
    private boolean isClosed;

    KeyedRow(long pointer) {
        this.pointer = pointer;
        this.isClosed = false;
    }

    public byte[] getKey() {
        return Binding.rowGetKey(pointer);
    }

    public boolean isNull(int index) {
        return Binding.rowIsNull(pointer, index);
    }

    public short getShort(int index) {
        return Binding.rowGetInt16Value(pointer, index);
    }

    public int getInt(int index) {
        return Binding.rowGetInt32Value(pointer, index);
    }

    public long getLong(int index) {
        return Binding.rowGetInt64Value(pointer, index);
    }

    public float getFloat(int index) {
        return Binding.rowGetFloatValue(pointer, index);
    }

    public double getDouble(int index) {
        return Binding.rowGetDoubleValue(pointer, index);
    }

    public boolean getBoolean(int index) {
        return Binding.rowGetBooleanValue(pointer, index);
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
