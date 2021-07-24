package com.risingwave.pgwire.database;

/** An authenticated database connection. */
public interface Database {
  PgResult runStatement(String sqlStmt) throws PgException;

  String getServerEncoding();
}
