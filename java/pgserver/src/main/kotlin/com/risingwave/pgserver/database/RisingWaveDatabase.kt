package com.risingwave.pgserver.database

import com.risingwave.catalog.CatalogService
import com.risingwave.common.exception.PgErrorCode
import com.risingwave.common.exception.PgException
import com.risingwave.execution.context.ExecutionContext
import com.risingwave.execution.context.FrontendEnv
import com.risingwave.execution.context.SessionConfiguration
import com.risingwave.pgwire.database.Database
import com.risingwave.pgwire.database.PgResult
import java.lang.Exception

/**
 * Create one for each session.
 */
class RisingWaveDatabase internal constructor(
  var frontendEnv: FrontendEnv,
  var database: String,
  user: String,
  var sessionConfiguration: SessionConfiguration
) : Database {
  @Throws(PgException::class)
  override suspend fun runStatement(sqlStmt: String): PgResult {
    val executionContext = ExecutionContext.builder()
      .withDatabase(database)
      .withSchema(CatalogService.DEFAULT_SCHEMA_NAME)
      .withFrontendEnv(frontendEnv)
      .withSessionConfig(sessionConfiguration)
      .build()
    return try {
      QueryExecution(executionContext, sqlStmt).call()
    } catch (e: Exception) {
      throw PgException(PgErrorCode.INTERNAL_ERROR, e)
    }
  }

  override val serverEncoding: String
    get() = "UTF-8"
}
