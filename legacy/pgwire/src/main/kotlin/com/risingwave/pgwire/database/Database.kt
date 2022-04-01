package com.risingwave.pgwire.database

import com.risingwave.common.exception.PgException

/** An authenticated database connection.  */
interface Database {
  @Throws(PgException::class)
  suspend fun runStatement(sqlStmt: String): PgResult

  val serverEncoding: String
}
