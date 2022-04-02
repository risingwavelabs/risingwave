package com.risingwave.execution.result.rpc.primitive;

import java.io.InputStream;

public class ShortBufferReader extends PrimitiveBufferReader<Short> {
  public ShortBufferReader(InputStream in) {
    super(in);
  }

  @Override
  protected Short doReadNext() throws Exception {
    return input.readShort();
  }
}
