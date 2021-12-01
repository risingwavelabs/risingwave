package com.risingwave.pgwire.duckdb.server;

import com.risingwave.pgwire.PgServer;
import com.risingwave.pgwire.database.Databases;
import com.risingwave.pgwire.duckdb.DuckDbManager;

/** Testing PgServer. */
public class DuckServer {
  public static void main(String[] args) {
    DuckDbManager duckDbManager = new DuckDbManager();
    Databases.setDatabaseManager(duckDbManager);

    PgServer srv = new PgServer("0.0.0.0", 12345, duckDbManager);
    srv.serve();
  }
}
