package com.risingwave.sql.node;

import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * AST node for `show A`. It is not found in Calcite so implement a self version. No operator yet
 * cuz it is hard code in SqlHandlerFactory to return.
 */
public class SqlShowParameters extends SqlCall {

  /**
   * Name of the option as an {@link org.apache.calcite.sql.SqlIdentifier} with one or more parts.
   */
  SqlIdentifier name;

  public SqlShowParameters(SqlParserPos pos, SqlIdentifier name) {
    super(pos);
    this.name = name;
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
    name.unparse(writer, leftPrec, rightPrec);
  }

  public SqlIdentifier getName() {
    return name;
  }
}
