package com.risingwave.planner.handler;

import com.risingwave.planner.context.ExecutionContext;
import org.apache.calcite.sql.SqlNode;

public interface SqlHandler {
  void handle(SqlNode ast, ExecutionContext context);
}
