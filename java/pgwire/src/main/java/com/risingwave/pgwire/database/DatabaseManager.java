package com.risingwave.pgwire.database;

/** The system that manages the databases. */
public interface DatabaseManager {
  Database connect(final String user, final String database) throws PgException;
}
