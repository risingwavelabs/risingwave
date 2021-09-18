package com.risingwave.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ExplainTest extends SqlTestBase {

  @BeforeAll
  public void initAll() {
    initEnv();
  }

  @Test
  @DisplayName("Test explain query")
  public void testExplainQuery() {
    executeSql("create table test_explain (v1 int not null)");

    // TODO: change to 'explain select 1' once supported
    var results = executeSql("explain select * from test_explain");
    var iterator = results.createIterator();

    assertTrue(iterator.next());
    var row = iterator.getRow();
    assertEquals(1, row.size());
    var content = row.get(0);
    assertTrue(content.encodeInText().contains("Scan"));
    assertFalse(iterator.next());

    executeSql("drop table test_explain");
  }
}
