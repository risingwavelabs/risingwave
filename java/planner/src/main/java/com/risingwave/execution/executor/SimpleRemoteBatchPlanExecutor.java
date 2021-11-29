package com.risingwave.execution.executor;

import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.pgwire.database.PgResult;
import com.risingwave.planner.rel.physical.BatchPlan;

/** Used to execute against single compute node. */
public class SimpleRemoteBatchPlanExecutor implements BatchPlanExecutor {
  @Override
  public PgResult execute(BatchPlan plan, ExecutionContext context) {
    return null;
  }
}
