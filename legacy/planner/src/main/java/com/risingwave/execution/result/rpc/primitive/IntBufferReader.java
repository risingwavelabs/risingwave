package com.risingwave.execution.result.rpc.primitive;

import java.io.InputStream;

public class IntBufferReader extends PrimitiveBufferReader<Integer> {
  public IntBufferReader(InputStream in) {
    super(in);
  }

  @Override
  protected Integer doReadNext() throws Exception {
    return input.readInt();
  }
}
