package com.risingwave.execution.handler;

import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.result.DdlResult;
import com.risingwave.pgwire.database.PgResult;
import com.risingwave.pgwire.msg.StatementType;
import org.apache.calcite.sql.SqlNode;

public class CreateStreamHandler implements SqlHandler {

  @Override
  public PgResult handle(SqlNode ast, ExecutionContext context) {
    return new DdlResult(StatementType.CREATE_STREAM, 0);
  }
}
