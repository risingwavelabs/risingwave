package com.risingwave.pgwire.msg;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * CancelRequest is a special StartupMessage that's sent from client after connection established.
 */
public final class CancelRequest extends PgMessage {
  CancelRequest() {
    super(PgMsgType.CancelRequest);
  }

  // CancelRequest
  // +---------------+-----------------+-----------------+--------------+
  // | int32 len(16) | int32(80877102) | int32 processID | int32 secret |
  // +---------------+-----------------+-----------------+--------------+
  @Override
  public void decodeFrom(byte[] buf) {
    ByteBuffer in = ByteBuffer.wrap(buf).order(ByteOrder.BIG_ENDIAN);
    processId = in.getInt();
    secret = in.getInt();
  }

  public int getSecret() {
    return secret;
  }

  public int getProcessId() {
    return processId;
  }

  private int processId;
  private int secret;
}
