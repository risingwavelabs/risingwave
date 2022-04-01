package com.risingwave.pgwire.database;

import com.risingwave.common.exception.PgException;

/** The system that manages the databases. */
public interface DatabaseManager {
  Database connect(final String user, final String database) throws PgException;
}
