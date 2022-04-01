package com.risingwave.execution.result.rpc.primitive;

import java.io.InputStream;
import java.sql.Timestamp;

public class TimestampBufferReader extends PrimitiveBufferReader<Timestamp> {
  public TimestampBufferReader(InputStream in) {
    super(in);
  }

  @Override
  protected Timestamp doReadNext() throws Exception {
    Long microSeconds = input.readLong();
    // Timestamp is stored as number of microseconds since Epoch 1970 (int64).
    // But now Java can not handle microseconds but milliseconds.
    return new Timestamp(microSeconds / 1000);
  }
}
