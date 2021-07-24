package com.risingwave.pgwire.msg;

import com.risingwave.pgwire.database.PgErrorCode;
import com.risingwave.pgwire.database.PgException;

public class PgMessage {
  /**
   * Read the detail message from buffer.
   *
   * @param buf the message body.
   */
  public void decodeFrom(byte[] buf) throws PgException {
    // The default implementation is for empty message, Terminate e.g.
    if (buf.length != 0) {
      throw new PgException(
          PgErrorCode.INTERNAL_ERROR,
          "subclass of PgMessage must override this method to decode message");
    }
  }

  public PgMessage(PgMsgType t) {
    this.type = t;
  }

  public PgMsgType getType() {
    return this.type;
  }

  private final PgMsgType type;
}
