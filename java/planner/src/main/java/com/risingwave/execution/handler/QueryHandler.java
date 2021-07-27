package com.risingwave.execution.handler;

import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.pgwire.database.PgResult;
import com.risingwave.planner.planner.batch.BatchPlanner;
import com.risingwave.planner.rel.physical.batch.BatchPlan;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

@HandlerSignature(sqlKinds = {SqlKind.SELECT})
public class QueryHandler implements SqlHandler {
  @Override
  public PgResult handle(SqlNode ast, ExecutionContext context) {
    BatchPlanner planner = new BatchPlanner();
    BatchPlan plan = planner.plan(ast, context);

    throw new UnsupportedOperationException();
  }
}
