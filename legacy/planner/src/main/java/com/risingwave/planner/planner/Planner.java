package com.risingwave.planner.planner;

import com.risingwave.execution.context.ExecutionContext;
import org.apache.calcite.sql.SqlNode;

public interface Planner<P> {
  P plan(SqlNode ast, ExecutionContext context);
}
