package com.risingwave.execution.result.rpc.primitive;

import static com.google.common.base.Preconditions.checkNotNull;

import com.risingwave.proto.data.Buffer;
import java.io.InputStream;

public class BufferReaders {
  public static InputStream decode(Buffer buffer) {
    checkNotNull(buffer);
    // TODO: Support more encoding and compression
    return buffer.getBody().newInput();
  }
}
