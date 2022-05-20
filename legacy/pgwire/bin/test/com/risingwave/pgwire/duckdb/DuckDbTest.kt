package com.risingwave.pgwire.duckdb

import com.risingwave.common.exception.PgException
import com.risingwave.pgwire.database.DatabaseManager
import com.risingwave.pgwire.database.PgResult
import com.risingwave.pgwire.types.PgValue
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.ArrayList

internal class DuckDbTest {
  @Test
  @Throws(PgException::class)
  suspend fun runStatementTest() {
    val db = dbManager.connect("", "")
    val res = db.runStatement("SELECT 1")
    val rows = mustGetAllRows(res)
    Assertions.assertEquals(rows.size, 1)
    Assertions.assertEquals(rows[0][0].encodeInText(), "1")
  }

  companion object {
    private var dbManager: DatabaseManager = DuckDbManager()

    fun mustGetAllRows(res: PgResult): List<List<PgValue>> {
      val ret: MutableList<List<PgValue>> = ArrayList()
      Assertions.assertDoesNotThrow {
        val it = res.createIterator()
        while (it.next()) {
          ret.add(it.row)
        }
      }
      return ret
    }
  }
}
