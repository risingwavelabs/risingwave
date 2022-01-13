package com.risingwave.sql.node;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.checkerframework.checker.nullness.qual.Nullable;

/** The calcite node for CREATE TABLE_V2 statement. */
public class SqlCreateTableV2 extends SqlCreateTable {

  public SqlCreateTableV2(
      SqlParserPos pos,
      boolean replace,
      boolean ifNotExists,
      SqlIdentifier name,
      @Nullable SqlNodeList columnList,
      @Nullable SqlNode query) {
    super(pos, replace, ifNotExists, name, columnList, query);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    writer.keyword("TABLE_V2");
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    name.unparse(writer, leftPrec, rightPrec);
    if (columnList != null) {
      SqlWriter.Frame frame = writer.startList("(", ")");
      for (SqlNode c : columnList) {
        writer.sep(",");
        c.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    }
    if (query != null) {
      writer.keyword("AS");
      writer.newlineAndIndent();
      query.unparse(writer, 0, 0);
    }
  }
}
