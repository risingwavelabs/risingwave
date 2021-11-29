package com.risingwave.scheduler;

import com.risingwave.planner.rel.physical.BatchPlan;
import java.util.concurrent.CompletableFuture;

/** Query manager interface for scheduling queries on remote workers. */
public interface QueryManager {
  CompletableFuture<QueryResultLocation> schedule(BatchPlan plan);
}
