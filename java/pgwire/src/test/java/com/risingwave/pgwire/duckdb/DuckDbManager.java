package com.risingwave.pgwire.duckdb;

import com.risingwave.pgwire.database.Database;
import com.risingwave.pgwire.database.DatabaseManager;
import com.risingwave.pgwire.database.PgErrorCode;
import com.risingwave.pgwire.database.PgException;
import java.sql.Connection;
import java.sql.DriverManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DuckDbManager implements DatabaseManager {
  private static final Logger log = LoggerFactory.getLogger(DuckDbManager.class);

  @Override
  public Database connect(String user, String database) throws PgException {
    try {
      Class.forName("org.duckdb.DuckDBDriver");
      Connection conn = DriverManager.getConnection("jdbc:duckdb:");
      return new JdbcDatabase(conn);
    } catch (Throwable exp) {
      log.error(exp.getMessage());
      throw new PgException(PgErrorCode.CONNECTION_FAILURE, exp);
    }
  }
}
