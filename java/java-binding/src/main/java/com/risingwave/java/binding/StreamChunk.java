package com.risingwave.java.binding;

public class StreamChunk implements AutoCloseable {
    private final long pointer;
    private boolean isClosed;

    StreamChunk(long pointer) {
        this.pointer = pointer;
        this.isClosed = false;
    }

    public static StreamChunk fromPayload(byte[] streamChunkPayload) {
        return new StreamChunk(Binding.newStreamChunkFromPayload(streamChunkPayload));
    }

    /**
     * This method generate the StreamChunk
     *
     * @param str A string that represent table format, content and operation. Example:"I I\n + 199
     *     40"
     */
    public static StreamChunk fromPretty(String str) {
        return new StreamChunk(Binding.newStreamChunkFromPretty(str));
    }

    @Override
    public void close() {
        if (!isClosed) {
            Binding.streamChunkClose(pointer);
            this.isClosed = true;
        }
    }

    long getPointer() {
        return this.pointer;
    }
}
