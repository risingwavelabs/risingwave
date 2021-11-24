package com.risingwave.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.risingwave.planner.sql.SqlConverter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class OperatorReturnTypeTest extends SqlTestBase {
  @BeforeAll
  public void init() {
    super.initEnv();
  }

  @ParameterizedTest(name = "{0} <- {1}")
  @CsvSource({
    "INTEGER NOT NULL, select 42",
    "INTEGER NOT NULL, select cast(1 as smallint) + 2",
    "INTEGER NOT NULL, select 2 + cast(1 as smallint)",
    "TIMESTAMP(0) NOT NULL, select date '1994-01-01' + interval '1' year",
  })
  @DisplayName("Operator return type tests")
  public void testReturnType(String expectedType, String sql) {
    var ast = super.parseSql(sql);
    SqlConverter sqlConverter = SqlConverter.builder(executionContext).build();
    var rawPlan = sqlConverter.toRel(ast).project();
    var returnType = rawPlan.getRowType().getFieldList().get(0).getType();
    assertEquals(expectedType, returnType.getFullTypeString());
  }
}
