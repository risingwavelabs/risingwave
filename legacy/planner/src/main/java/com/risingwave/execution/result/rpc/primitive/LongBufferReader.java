package com.risingwave.execution.result.rpc.primitive;

import java.io.InputStream;

public class LongBufferReader extends PrimitiveBufferReader<Long> {
  public LongBufferReader(InputStream in) {
    super(in);
  }

  @Override
  protected Long doReadNext() throws Exception {
    return input.readLong();
  }
}
