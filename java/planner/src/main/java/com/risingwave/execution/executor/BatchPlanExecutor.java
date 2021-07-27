package com.risingwave.execution.executor;

import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.pgwire.database.PgResult;
import com.risingwave.planner.rel.physical.batch.BatchPlan;

public interface BatchPlanExecutor {
  PgResult execute(BatchPlan plan, ExecutionContext context);
}
