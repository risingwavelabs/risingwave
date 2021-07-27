package com.risingwave.execution.result.rpc;

import com.risingwave.execution.result.rpc.primitive.BooleanBufferReader;
import com.risingwave.pgwire.types.PgValue;
import javax.annotation.Nullable;

public abstract class PgValueReaderBase implements PgValueReader {
  private final BooleanBufferReader nullBitmapReader;

  public PgValueReaderBase(@Nullable BooleanBufferReader nullBitmapReader) {
    this.nullBitmapReader = nullBitmapReader;
  }

  @Override
  public PgValue next() {
    PgValue value = nextValue();
    boolean isNull = (nullBitmapReader != null) && nullBitmapReader.next();
    return isNull ? null : value;
  }

  protected abstract PgValue nextValue();
}
