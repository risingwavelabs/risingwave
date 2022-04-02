package com.risingwave.pgwire.duckdb

import com.risingwave.common.exception.PgErrorCode
import com.risingwave.common.exception.PgException
import com.risingwave.pgwire.database.Database
import com.risingwave.pgwire.database.PgResult
import java.lang.Exception
import java.sql.Connection

class JdbcDatabase internal constructor(private val conn: Connection) : Database {
  @Throws(PgException::class)
  override suspend fun runStatement(sqlStmt: String): PgResult {
    return try {
      val stmt = conn.createStatement()
      val hasResult = stmt.execute(sqlStmt)
      if (hasResult) {
        JdbcResult(sqlStmt, stmt.resultSet)
      } else JdbcResult(sqlStmt, stmt.updateCount)
    } catch (exp: Exception) {
      throw PgException(PgErrorCode.INTERNAL_ERROR, exp)
    }
  }

  override val serverEncoding: String
    get() = "utf8"
}
