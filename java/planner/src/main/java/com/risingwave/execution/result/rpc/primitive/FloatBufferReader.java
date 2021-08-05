package com.risingwave.execution.result.rpc.primitive;

import java.io.InputStream;

public class FloatBufferReader extends PrimitiveBufferReader<Float> {
  public FloatBufferReader(InputStream in) {
    super(in);
  }

  @Override
  protected Float doReadNext() throws Exception {
    return input.readFloat();
  }
}
