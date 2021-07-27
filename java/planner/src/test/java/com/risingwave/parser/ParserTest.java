package com.risingwave.parser;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.junit.jupiter.api.Test;

public class ParserTest {
  @Test
  public void testCreateTable() {
    assertTrue(
        SqlParser.createStatement("create table t (a int not null);") instanceof SqlCreateTable);
    assertTrue(
        SqlParser.createStatement(
                "create table t (a int not null, b int not null, c int not null);")
            instanceof SqlCreateTable);
  }

  @Test
  public void testInsertValues() {
    assertTrue(SqlParser.createStatement("insert into t values (1);") instanceof SqlInsert);
    assertTrue(SqlParser.createStatement("insert into t values (1, 2, 3);") instanceof SqlInsert);
    assertTrue(
        SqlParser.createStatement("insert into t values (1, 2, 3), (4, 5, 6);")
            instanceof SqlInsert);
  }

  @Test
  public void testSimpleSelect() {
    assertTrue(SqlParser.createStatement("select a, b from t;") instanceof SqlSelect);
    assertTrue(SqlParser.createStatement("select a from t where b = 1") instanceof SqlSelect);
    assertTrue(SqlParser.createStatement("select a from t where b >= 1") instanceof SqlSelect);
    assertTrue(SqlParser.createStatement("select a from t where b > 1") instanceof SqlSelect);
    assertTrue(SqlParser.createStatement("select a from t where b <= 1") instanceof SqlSelect);
    assertTrue(SqlParser.createStatement("select a from t where b < 1") instanceof SqlSelect);
    assertTrue(SqlParser.createStatement("select a from t where b != 1") instanceof SqlSelect);
  }

  private static void assertStatement(String query, SqlNode expected) {
    assertParsed(query, expected, SqlParser.createStatement(query));
  }

  private static void assertParsed(String input, SqlNode expected, SqlNode parsed) {
    if (!parsed.equals(expected)) {
      fail(
          format(
              "expected%n%n%s%n%nto parse as%n%n%s%n%nbut was%n%n%s%n",
              indent(input), indent(expected.toString()), indent(parsed.toString())));
    }
  }

  private static String indent(String value) {
    String indent = "    ";
    return indent + value.trim().replaceAll("\n", "\n" + indent);
  }
}
