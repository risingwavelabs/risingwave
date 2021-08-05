package com.risingwave.execution.result.rpc.primitive;

import com.google.common.base.Charsets;
import java.io.InputStream;

public class CharBufferReader extends PrimitiveBufferReader<String> {
  private final int maxLen;
  private final byte[] valueBuffer;

  public CharBufferReader(InputStream in, int maxLen) {
    super(in);
    this.maxLen = maxLen;
    this.valueBuffer = new byte[maxLen];
  }

  @Override
  protected String doReadNext() throws Exception {
    input.read(this.valueBuffer);
    return new String(this.valueBuffer, 0, this.valueBuffer.length, Charsets.UTF_8);
  }
}
