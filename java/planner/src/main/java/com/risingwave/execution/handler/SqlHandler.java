package com.risingwave.execution.handler;

import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.pgwire.database.PgResult;
import com.risingwave.pgwire.msg.StatementType;
import org.apache.calcite.sql.SqlNode;

public interface SqlHandler {
  PgResult handle(SqlNode ast, ExecutionContext context);

  static StatementType getStatementType(SqlNode ast) {
    switch (ast.getKind()) {
      case INSERT:
        return StatementType.INSERT;
      case DELETE:
        return StatementType.DELETE;
      case UPDATE:
        return StatementType.UPDATE;
      case SELECT:
        return StatementType.SELECT;
      case ORDER_BY:
        return StatementType.ORDER_BY;
      case OTHER:
        return StatementType.OTHER;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported statement type %s", ast.getKind()));
    }
  }
}
