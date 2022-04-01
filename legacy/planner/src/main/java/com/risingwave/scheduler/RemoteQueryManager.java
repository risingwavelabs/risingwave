package com.risingwave.scheduler;

import static java.util.Objects.requireNonNull;

import com.risingwave.planner.rel.physical.BatchPlan;
import com.risingwave.scheduler.query.PlanFragmenter;
import com.risingwave.scheduler.query.Query;
import com.risingwave.scheduler.query.QueryId;
import com.risingwave.scheduler.task.TaskManager;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.inject.Inject;
import javax.inject.Singleton;

/** Manages the incoming queries. */
@Singleton
public class RemoteQueryManager implements QueryManager {
  private final TaskManager taskManager;
  private final ResourceManager resourceManager;

  private final ConcurrentMap<QueryId, QueryExecution> queries = new ConcurrentHashMap<>();

  @Inject
  public RemoteQueryManager(TaskManager taskManager, ResourceManager resourceManager) {
    this.taskManager = requireNonNull(taskManager, "taskManager");
    this.resourceManager = requireNonNull(resourceManager, "resourceManager");
  }

  @Override
  public CompletableFuture<QueryResultLocation> schedule(BatchPlan plan, long epoch) {
    CompletableFuture<QueryResultLocation> resultFuture = new CompletableFuture<>();
    createQueryExecution(plan, resultFuture, epoch);
    return resultFuture;
  }

  private void createQueryExecution(
      BatchPlan plan, CompletableFuture<QueryResultLocation> resultFuture, long epoch) {
    Query query = PlanFragmenter.planDistribution(plan);

    QueryExecution queryExecution =
        new QueryExecution(query, taskManager, resourceManager, resultFuture, epoch);
    queryExecution.start();
    queries.put(query.getQueryId(), queryExecution);
  }
}
