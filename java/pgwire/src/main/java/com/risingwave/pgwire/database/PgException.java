package com.risingwave.pgwire.database;

/** PgException is the postgres-specific exception. Every instance binds with an error code. */
public class PgException extends Exception {

  public PgException(PgErrorCode code, String format, Object... args) {
    super(String.format(format, args));
    this.code = code;
  }

  /** Construct from an existing exception, IOException, e.g. */
  public PgException(PgErrorCode code, Throwable exp) {
    super(exp);
    this.code = code;
  }

  public PgErrorCode getCode() {
    return code;
  }

  private final PgErrorCode code;
}
