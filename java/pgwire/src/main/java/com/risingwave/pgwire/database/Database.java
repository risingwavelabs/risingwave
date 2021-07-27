package com.risingwave.pgwire.database;

import com.risingwave.common.exception.PgException;

/** An authenticated database connection. */
public interface Database {
  PgResult runStatement(String sqlStmt) throws PgException;

  String getServerEncoding();
}
