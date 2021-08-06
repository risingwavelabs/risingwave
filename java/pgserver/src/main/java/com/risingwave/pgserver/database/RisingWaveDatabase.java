package com.risingwave.pgserver.database;

import com.risingwave.catalog.CatalogService;
import com.risingwave.common.config.Configuration;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.handler.SqlHandlerFactory;
import com.risingwave.pgwire.database.Database;
import com.risingwave.pgwire.database.PgResult;

public class RisingWaveDatabase implements Database {
  private final Configuration sessionConf;
  private final CatalogService catalogService;
  private final String database;
  private final String user;
  private final SqlHandlerFactory sqlHandlerFactory;
  private String schema = CatalogService.DEFAULT_SCHEMA_NAME;

  RisingWaveDatabase(
      Configuration sessionConf,
      CatalogService catalogService,
      String database,
      String user,
      SqlHandlerFactory sqlHandlerFactory) {
    this.sessionConf = sessionConf;
    this.catalogService = catalogService;
    this.database = database;
    this.user = user;
    this.sqlHandlerFactory = sqlHandlerFactory;
  }

  @Override
  public PgResult runStatement(String sqlStmt) throws PgException {
    ExecutionContext executionContext =
        ExecutionContext.builder()
            .withDatabase(database)
            .withSchema(schema)
            .withCatalogService(catalogService)
            .withConfiguration(sessionConf)
            .withSqlHandlerFactory(sqlHandlerFactory)
            .build();

    try {
      return new QueryExecution(executionContext, sqlStmt).call();
    } catch (Exception e) {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, e);
    }
  }

  @Override
  public String getServerEncoding() {
    // TODO: Fix this.
    return "UTF-8";
  }
}
