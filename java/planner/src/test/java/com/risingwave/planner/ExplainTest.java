package com.risingwave.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/** Test of Explain */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ExplainTest extends SqlTestBase {

  @BeforeAll
  public void initAll() {
    initEnv();
    executeSql("create table test_explain (v1 int not null)");
  }

  @AfterAll
  public void clean() {
    executeSql("drop table test_explain");
  }

  @Test
  @DisplayName("explain select query")
  public void testExplainQuery() {
    var results = executeSql("explain select * from test_explain");
    var iterator = results.createIterator();

    assertTrue(iterator.next());
    assertTrue(iterator.next());
    var row = iterator.getRow();
    assertEquals(1, row.size());
    assertTrue(row.get(0).encodeInText().contains("Scan"));
    assertFalse(iterator.next());
  }

  @Test
  @DisplayName("explain create materialized view")
  public void testExplainCreateMaterializedView() {
    var results =
        executeSql("explain create materialized view test_mv as select * from test_explain");
    var iterator = results.createIterator();

    assertTrue(iterator.next());
    var row1 = iterator.getRow();
    assertEquals(1, row1.size());
    assertTrue(row1.get(0).encodeInText().contains("StreamMaterializedView"));

    assertTrue(iterator.next());
    var row2 = iterator.getRow();
    assertTrue(row2.get(0).encodeInText().contains("StreamTableSource"));
    assertFalse(iterator.next());
  }
}
