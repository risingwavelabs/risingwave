package com.risingwave.execution.result.rpc;

import com.risingwave.execution.result.rpc.primitive.BooleanBufferReader;
import com.risingwave.execution.result.rpc.primitive.PrimitiveBufferReader;
import com.risingwave.pgwire.types.PgValue;
import java.util.function.Function;
import javax.annotation.Nullable;

public class PrimitiveValueReader<T> extends PgValueReaderBase {
  private final PrimitiveBufferReader<T> values;
  private final Function<T, PgValue> transformer;

  private PrimitiveValueReader(
      Function<T, PgValue> transformer,
      PrimitiveBufferReader<T> values,
      @Nullable BooleanBufferReader nullBitmapReader) {
    super(nullBitmapReader);
    this.values = values;
    this.transformer = transformer;
  }

  @Override
  protected PgValue nextValue() {
    return transformer.apply(values.next());
  }

  public static <T> PrimitiveValueReader<T> createValueReader(
      Function<T, PgValue> transformer,
      PrimitiveBufferReader<T> values,
      @Nullable BooleanBufferReader nullBitmap) {
    return new PrimitiveValueReader<>(transformer, values, nullBitmap);
  }
}
