package com.risingwave.sql.node;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.risingwave.sql.parser.SqlParser;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.junit.jupiter.api.Test;

public class ParseSqlNodeTest {
  @Test
  public void testCreateStream() {
    SqlNode node =
        SqlParser.createCalciteStatement(
            "create stream s (v1 int not null) with (property_a = 'a') ROW FORMAT json");

    var writer = new SqlPrettyWriter();
    node.unparse(writer, 0, 0);
    assertEquals(
        writer.toSqlString().getSql(),
        "CREATE STREAM IF NOT EXISTS \"s\" (\"v1\" INTEGER NOT NULL) WITH ('property_a' = 'a') ROW FORMAT json");
  }
}
