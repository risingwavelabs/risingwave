package com.risingwave.scheduler;

import static java.util.Objects.requireNonNull;

import com.risingwave.planner.rel.physical.batch.BatchPlan;
import com.risingwave.scheduler.actor.ActorFactory;
import com.risingwave.scheduler.query.PlanFragmenter;
import com.risingwave.scheduler.query.Query;
import com.risingwave.scheduler.query.QueryExecution;
import com.risingwave.scheduler.query.QueryId;
import com.risingwave.scheduler.query.RemoteQueryExecution;
import com.risingwave.scheduler.task.TaskManager;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class RemoteQueryManager implements QueryManager {
  private final TaskManager taskManager;
  private final ActorFactory actorFactory;

  private final ConcurrentMap<QueryId, QueryExecution> queries = new ConcurrentHashMap<>();

  @Inject
  public RemoteQueryManager(TaskManager taskManager, ActorFactory actorFactory) {
    this.taskManager = requireNonNull(taskManager, "taskManager");
    this.actorFactory = requireNonNull(actorFactory, "actorFactory");
  }

  @Override
  public CompletableFuture<QueryResultLocation> schedule(BatchPlan plan) {
    CompletableFuture<QueryResultLocation> resultFuture = new CompletableFuture<>();
    createQueryExecution(plan, resultFuture);
    return resultFuture;
  }

  private void createQueryExecution(
      BatchPlan plan, CompletableFuture<QueryResultLocation> resultFuture) {
    PlanFragmenter fragmenter = new PlanFragmenter(plan);
    Query query = fragmenter.planDistribution();

    QueryExecution queryExecution =
        new RemoteQueryExecution(query, actorFactory, taskManager, resultFuture);
    queryExecution.start();
    queries.put(query.getQueryId(), queryExecution);
  }
}
