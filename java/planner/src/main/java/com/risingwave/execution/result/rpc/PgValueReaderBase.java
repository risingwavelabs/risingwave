package com.risingwave.execution.result.rpc;

import com.risingwave.execution.result.rpc.primitive.BooleanBufferReader;
import com.risingwave.pgwire.types.PgValue;
import javax.annotation.Nullable;

/** Read value from data chunk */
public abstract class PgValueReaderBase implements PgValueReader {
  private final BooleanBufferReader nullBitmapReader;

  public PgValueReaderBase(@Nullable BooleanBufferReader nullBitmapReader) {
    this.nullBitmapReader = nullBitmapReader;
  }

  @Override
  public PgValue next() {
    boolean isNull = (nullBitmapReader != null) && !nullBitmapReader.next();
    return isNull ? null : nextValue();
  }

  protected abstract PgValue nextValue();
}
