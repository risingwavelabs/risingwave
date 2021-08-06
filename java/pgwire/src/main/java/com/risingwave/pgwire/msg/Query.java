package com.risingwave.pgwire.msg;

import java.nio.charset.StandardCharsets;

public final class Query extends PgMessage {
  public Query() {
    super(PgMsgType.Query);
  }

  // Query
  // +-----+-----------+-----------+
  // | 'Q' | int32 len | str query |
  // +-----+-----------+-----------+
  @Override
  public void decodeFrom(byte[] buf) {
    // TODO(TaoWu): Support customizable client encoding.
    //              https://www.postgresql.org/docs/13/multibyte.html

    // For queries, pg client will send a \0 at the end of query, so we should ignore the last byte
    sqlStatement = new String(buf, StandardCharsets.US_ASCII).trim();
  }

  public String getSql() {
    return sqlStatement;
  }

  private String sqlStatement;
}
