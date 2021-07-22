package com.risingwave.planner.planner;

import org.apache.calcite.sql.SqlNode;

public interface Planner<P> {
  P plan(SqlNode ast, PlannerContext context);
}
