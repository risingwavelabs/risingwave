package com.risingwave.execution.result.rpc.primitive;

import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import java.io.DataInputStream;
import java.io.InputStream;

public abstract class PrimitiveBufferReader<T> {
  protected final DataInputStream input;

  protected PrimitiveBufferReader(InputStream in) {
    this.input = new DataInputStream(in);
  }

  public T next() {
    try {
      return doReadNext();
    } catch (Exception e) {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, e);
    }
  }

  protected abstract T doReadNext() throws Exception;
}
