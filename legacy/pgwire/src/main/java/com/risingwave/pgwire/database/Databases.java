package com.risingwave.pgwire.database;

import com.risingwave.common.exception.PgException;

public class Databases {
  private static DatabaseManager singleton;

  public static Database connect(final String user, final String database) throws PgException {
    return singleton.connect(user, database);
  }

  // NOTE: Maybe we should use SPI for this.
  public static void setDatabaseManager(DatabaseManager manager) {
    singleton = manager;
  }
}
