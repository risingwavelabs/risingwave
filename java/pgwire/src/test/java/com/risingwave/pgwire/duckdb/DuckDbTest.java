package com.risingwave.pgwire.duckdb;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.risingwave.pgwire.database.Database;
import com.risingwave.pgwire.database.Databases;
import com.risingwave.pgwire.database.PgException;
import com.risingwave.pgwire.database.PgResult;
import com.risingwave.pgwire.types.PgValue;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DuckDbTest {
  @BeforeAll
  static void setup() {
    Databases.setDatabaseManager(new DuckDbManager());
  }

  static List<List<PgValue>> mustGetAllRows(PgResult res) {
    List<List<PgValue>> ret = new ArrayList<>();
    assertDoesNotThrow(
        () -> {
          PgResult.PgIter it = res.createIterator();
          while (it.next()) {
            ret.add(it.getRow());
          }
        });
    return ret;
  }

  @Test
  void runStatementTest() throws PgException {
    Database db = Databases.connect("", "");
    PgResult res = db.runStatement("SELECT 1");
    List<List<PgValue>> rows = mustGetAllRows(res);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).get(0).encodeInText(), "1");
  }
}
