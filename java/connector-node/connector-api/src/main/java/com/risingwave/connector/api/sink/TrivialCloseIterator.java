package com.risingwave.connector.api.sink;

import java.util.Iterator;

public class TrivialCloseIterator<E> implements CloseableIterator<E> {

    private final Iterator<E> inner;

    public TrivialCloseIterator(Iterator<E> inner) {
        this.inner = inner;
    }

    @Override
    public void close() throws Exception {}

    @Override
    public boolean hasNext() {
        return inner.hasNext();
    }

    @Override
    public E next() {
        return inner.next();
    }
}
