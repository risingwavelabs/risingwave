package com.risingwave.execution.result.rpc.primitive;

import java.io.InputStream;
import java.util.Date;

public class DateBufferReader extends PrimitiveBufferReader<Date> {
  public DateBufferReader(InputStream in) {
    super(in);
  }

  @Override
  protected Date doReadNext() throws Exception {
    int days = input.readInt();
    // Date is stored as number of days since Epoch 1970 (int32). Convert to milliseconds.
    long miliSecs = ((long) days) * 86400_000;
    return new Date(miliSecs);
  }
}
