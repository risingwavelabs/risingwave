package com.risingwave.java.binding;

public class CdcJniChannel implements AutoCloseable {
    private final long pointer;
    private boolean isClosed;

    public CdcJniChannel(long pointer) {
        this.pointer = pointer;
        this.isClosed = false;
    }

    public long getPointer() {
        return this.pointer;
    }

    @Override
    public void close() {
        if (!isClosed) {
            isClosed = true;
            Binding.cdcJniChannelClose(pointer);
        }
    }
}
