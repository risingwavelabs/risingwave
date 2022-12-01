package com.risingwave.binding;

public class Iterator implements AutoCloseable {
    final long pointer;
    public Iterator() {
        this.pointer = Binding.iteratorNew();
    }

    public NextResult next() {
        return Binding.iteratorNext(pointer);
    }

    @Override
    public void close() {
        Binding.iteratorClose(pointer);
    }
}
