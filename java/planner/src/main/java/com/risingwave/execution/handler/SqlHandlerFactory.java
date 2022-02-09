package com.risingwave.execution.handler;

import com.risingwave.execution.context.ExecutionContext;
import org.apache.calcite.sql.SqlNode;

/** Interface for SQL handler factory. */
public interface SqlHandlerFactory {
  SqlHandler create(SqlNode ast, ExecutionContext context);

  void setUseV2(boolean useV2);
}
