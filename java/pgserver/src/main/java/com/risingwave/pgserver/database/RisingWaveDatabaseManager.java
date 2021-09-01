package com.risingwave.pgserver.database;

import com.risingwave.common.exception.PgException;
import com.risingwave.execution.context.FrontendEnv;
import com.risingwave.pgwire.database.Database;
import com.risingwave.pgwire.database.DatabaseManager;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class RisingWaveDatabaseManager implements DatabaseManager {
  private final FrontendEnv frontendEnv;

  @Inject
  public RisingWaveDatabaseManager(FrontendEnv ctx) {
    this.frontendEnv = ctx;
  }

  @Override
  public Database connect(String user, String database) throws PgException {
    return new RisingWaveDatabase(frontendEnv, database, user);
  }
}
