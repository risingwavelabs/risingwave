package com.risingwave.pgwire.duckdb;

import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.pgwire.database.Database;
import com.risingwave.pgwire.database.PgResult;
import java.sql.Connection;
import java.sql.Statement;

public class JdbcDatabase implements Database {
  JdbcDatabase(Connection conn) {
    this.conn = conn;
  }

  @Override
  public PgResult runStatement(String sqlStmt) throws PgException {
    try {
      Statement stmt = conn.createStatement();
      boolean hasResult = stmt.execute(sqlStmt);
      if (hasResult) {
        return new JdbcResult(sqlStmt, stmt.getResultSet());
      }
      return new JdbcResult(sqlStmt, stmt.getUpdateCount());
    } catch (Exception exp) {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, exp);
    }
  }

  @Override
  public String getServerEncoding() {
    return "utf8";
  }

  private final Connection conn;
}
