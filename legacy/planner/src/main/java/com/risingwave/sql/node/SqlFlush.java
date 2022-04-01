package com.risingwave.sql.node;

import java.util.List;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

/** The calcite node for FLUSH statement. */
public class SqlFlush extends SqlCall {

  public SqlFlush(SqlParserPos pos) {
    super(pos);
  }

  @Override
  public SqlOperator getOperator() {
    return null;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return null;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("FLUSH");
  }
}
