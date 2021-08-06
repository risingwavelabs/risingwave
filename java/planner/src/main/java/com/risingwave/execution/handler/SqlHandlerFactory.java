package com.risingwave.execution.handler;

import com.risingwave.execution.context.ExecutionContext;
import org.apache.calcite.sql.SqlNode;

public interface SqlHandlerFactory {
  SqlHandler create(SqlNode ast, ExecutionContext context);
}
