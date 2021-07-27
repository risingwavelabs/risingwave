package com.risingwave.execution.handler;

import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.pgwire.database.PgResult;
import org.apache.calcite.sql.SqlNode;

public interface SqlHandler {
  PgResult handle(SqlNode ast, ExecutionContext context);
}
