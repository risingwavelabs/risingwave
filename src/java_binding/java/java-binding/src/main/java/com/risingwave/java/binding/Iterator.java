package com.risingwave.java.binding;

public class Iterator implements AutoCloseable {
    final long pointer;
    boolean isClosed;

    public Iterator() {
        this.pointer = Binding.iteratorNew();
        this.isClosed = false;
    }

    public Record next() {
        long pointer = Binding.iteratorNext(this.pointer);
        if (pointer == 0) {
            return null;
        }
        return new Record(pointer);
    }

    @Override
    public void close() {
        if (!isClosed) {
            isClosed = true;
            Binding.iteratorClose(pointer);
        }
    }
}
