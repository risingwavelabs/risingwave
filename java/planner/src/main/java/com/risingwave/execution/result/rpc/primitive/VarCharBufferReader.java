package com.risingwave.execution.result.rpc.primitive;

import com.google.common.base.Charsets;
import java.io.InputStream;

public class VarCharBufferReader extends PrimitiveBufferReader<String> {
  private final int maxLen;
  private final byte[] valueBuffer;

  public VarCharBufferReader(InputStream in, int maxLen) {
    super(in);
    this.maxLen = maxLen;
    this.valueBuffer = new byte[maxLen];
  }

  @Override
  protected String doReadNext() throws Exception {
    Short dataLen = input.readShort();
    input.read(this.valueBuffer);
    return new String(this.valueBuffer, 0, dataLen, Charsets.UTF_8);
  }
}
