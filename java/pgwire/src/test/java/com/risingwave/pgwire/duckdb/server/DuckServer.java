package com.risingwave.pgwire.duckdb.server;

import com.risingwave.pgwire.PgServer;
import com.risingwave.pgwire.database.Databases;
import com.risingwave.pgwire.duckdb.DuckDbManager;

public class DuckServer {
  public static void main(String[] args) {
    Databases.setDatabaseManager(new DuckDbManager());

    PgServer srv = new PgServer(12345);
    srv.serve();
  }
}
