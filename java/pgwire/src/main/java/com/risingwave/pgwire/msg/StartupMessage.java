package com.risingwave.pgwire.msg;

import com.risingwave.pgwire.database.PgErrorCode;
import com.risingwave.pgwire.database.PgException;
import java.nio.charset.StandardCharsets;

// Startup Message
// +-----------+----------------+----------+----+-----------+-----+----+
// | int32 len | int32 protocol | str name | \0 | str value | ... | \0 |
// +-----------+----------------+----------+----+-----------+-----+----+
public class StartupMessage extends PgMessage {
  public StartupMessage() {
    super(PgMsgType.StartupMessage);
  }

  @Override
  public void decodeFrom(byte[] buf) throws PgException {
    String payload = new String(buf, StandardCharsets.US_ASCII);
    String[] segments = payload.split("\0");
    if (segments.length % 2 != 0) {
      throw new PgException(PgErrorCode.PROTOCOL_VIOLATION, "invalid startup packet");
    }

    for (int i = 0; i < segments.length; i += 2) {
      String name = segments[i];
      String value = segments[i + 1];
      if (name.equals("user")) {
        this.user = value;
      } else if (name.equals("database")) {
        this.database = value;
      }
      // Ignore other attributes.
    }
  }

  public String getUser() {
    return user;
  }

  public String getDatabase() {
    return database;
  }

  private String user;
  private String database;
}
