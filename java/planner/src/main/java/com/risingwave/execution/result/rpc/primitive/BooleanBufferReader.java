package com.risingwave.execution.result.rpc.primitive;

import java.io.InputStream;

public class BooleanBufferReader extends PrimitiveBufferReader<Boolean> {
  public BooleanBufferReader(InputStream in) {
    super(in);
  }

  @Override
  protected Boolean doReadNext() throws Exception {
    return input.readByte() != 0;
  }
}
