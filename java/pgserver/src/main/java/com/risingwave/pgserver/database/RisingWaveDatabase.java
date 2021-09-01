package com.risingwave.pgserver.database;

import com.risingwave.catalog.CatalogService;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.context.FrontendEnv;
import com.risingwave.pgwire.database.Database;
import com.risingwave.pgwire.database.PgResult;

public class RisingWaveDatabase implements Database {
  FrontendEnv frontendEnv;
  String database;

  RisingWaveDatabase(FrontendEnv frontendEnv, String database, String user) {
    this.frontendEnv = frontendEnv;
    this.database = database;
  }

  @Override
  public PgResult runStatement(String sqlStmt) throws PgException {
    ExecutionContext executionContext =
        ExecutionContext.builder()
            .withDatabase(database)
            .withSchema(CatalogService.DEFAULT_SCHEMA_NAME)
            .withFrontendEnv(frontendEnv)
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
