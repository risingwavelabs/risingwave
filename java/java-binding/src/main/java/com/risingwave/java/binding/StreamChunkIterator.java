package com.risingwave.java.binding;

public class StreamChunkIterator implements AutoCloseable {
    private final long pointer;
    private boolean isClosed;

    public StreamChunkIterator(byte[] streamChunkPayload) {
        this.pointer = Binding.streamChunkIteratorNew(streamChunkPayload);
        this.isClosed = false;
    }

    public StreamChunkRow next() {
        long pointer = Binding.streamChunkIteratorNext(this.pointer);
        if (pointer == 0) {
            return null;
        }
        return new StreamChunkRow(pointer);
    }

    @Override
    public void close() {
        if (!isClosed) {
            isClosed = true;
            Binding.streamChunkIteratorClose(pointer);
        }
    }
}