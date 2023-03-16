package com.risingwave.connector.api.sink;

import java.util.Iterator;

public interface CloseableIterator<E> extends AutoCloseable, Iterator<E> {}
