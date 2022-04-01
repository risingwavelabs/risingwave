package com.risingwave.sql.node;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.risingwave.sql.parser.SqlParser;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.junit.jupiter.api.Test;

/** Some example tests for testing create source statements. */
public class ParseSqlNodeTest {
  @Test
  public void testCreateSource() {
    SqlNode node =
        SqlParser.createCalciteStatement(
            "create source s (v1 int not null) with (property_a = 'a') ROW FORMAT json");

    var writer = new SqlPrettyWriter();
    node.unparse(writer, 0, 0);
    assertEquals(
        writer.toSqlString().getSql(),
        "CREATE SOURCE IF NOT EXISTS \"s\" (\"v1\" INTEGER NOT NULL) WITH ('property_a' = 'a') ROW FORMAT json");
  }
}
