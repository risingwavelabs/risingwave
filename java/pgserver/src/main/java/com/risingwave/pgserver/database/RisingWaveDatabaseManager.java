package com.risingwave.pgserver.database;

import com.risingwave.catalog.CatalogService;
import com.risingwave.common.config.Configuration;
import com.risingwave.common.exception.PgException;
import com.risingwave.execution.handler.SqlHandlerFactory;
import com.risingwave.pgwire.database.Database;
import com.risingwave.pgwire.database.DatabaseManager;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class RisingWaveDatabaseManager implements DatabaseManager {
  private final Configuration configuration;
  private final CatalogService catalogService;
  private final SqlHandlerFactory sqlHandlerFactory;

  @Inject
  public RisingWaveDatabaseManager(
      Configuration configuration,
      CatalogService catalogService,
      SqlHandlerFactory sqlHandlerFactory) {
    this.configuration = configuration;
    this.catalogService = catalogService;
    this.sqlHandlerFactory = sqlHandlerFactory;
  }

  @Override
  public Database connect(String user, String database) throws PgException {
    return new RisingWaveDatabase(configuration, catalogService, database, user, sqlHandlerFactory);
  }
}
