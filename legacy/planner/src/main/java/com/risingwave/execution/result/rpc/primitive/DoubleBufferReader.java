package com.risingwave.execution.result.rpc.primitive;

import java.io.InputStream;

public class DoubleBufferReader extends PrimitiveBufferReader<Double> {
  public DoubleBufferReader(InputStream in) {
    super(in);
  }

  @Override
  protected Double doReadNext() throws Exception {
    return input.readDouble();
  }
}
