package com.risingwave.execution.executor;

import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.pgwire.database.PgResult;
import com.risingwave.planner.rel.physical.BatchPlan;

/** Batch plan executor interface. TODO: seems useless after introducing scheduler. */
public interface BatchPlanExecutor {
  PgResult execute(BatchPlan plan, ExecutionContext context);
}
