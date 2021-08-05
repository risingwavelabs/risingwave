package com.risingwave.scheduler;

import com.risingwave.planner.rel.physical.batch.BatchPlan;
import java.util.concurrent.CompletableFuture;

public interface QueryManager {
  CompletableFuture<QueryResultLocation> schedule(BatchPlan plan);
}
