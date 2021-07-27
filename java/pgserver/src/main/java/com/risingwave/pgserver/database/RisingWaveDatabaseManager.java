package com.risingwave.pgserver.database;

import com.risingwave.catalog.CatalogService;
import com.risingwave.common.config.Configuration;
import com.risingwave.common.exception.PgException;
import com.risingwave.pgwire.database.Database;
import com.risingwave.pgwire.database.DatabaseManager;

public class RisingWaveDatabaseManager implements DatabaseManager {
  private final Configuration configuration;
  private final CatalogService catalogService;

  public RisingWaveDatabaseManager(Configuration configuration, CatalogService catalogService) {
    this.configuration = configuration;
    this.catalogService = catalogService;
  }

  @Override
  public Database connect(String user, String database) throws PgException {
    return new RisingWaveDatabase(configuration, catalogService, database, user);
  }
}
