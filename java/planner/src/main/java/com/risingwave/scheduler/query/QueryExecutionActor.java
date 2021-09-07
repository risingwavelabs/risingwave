package com.risingwave.scheduler.query;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Receive;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.node.WorkerNode;
import com.risingwave.scheduler.EventListener;
import com.risingwave.scheduler.QueryResultLocation;
import com.risingwave.scheduler.actor.ActorFactory;
import com.risingwave.scheduler.stage.QueryStage;
import com.risingwave.scheduler.stage.RemoteStageExecution;
import com.risingwave.scheduler.stage.ScheduledStage;
import com.risingwave.scheduler.stage.StageEvent;
import com.risingwave.scheduler.stage.StageExecution;
import com.risingwave.scheduler.stage.StageId;
import com.risingwave.scheduler.task.TaskId;
import com.risingwave.scheduler.task.TaskManager;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Schedules a query. <br>
 * The stages scheduling goes from bottom (leaf stages) to top (root stage). <br>
 * To start a stage, all its dependencies must have been scheduled, because their physical
 * properties, like node assignment information, are required. For example, Exchange requires the
 * children's ExchangeSource, which couldn't be known unless scheduled.
 */
public class QueryExecutionActor extends AbstractBehavior<QueryExecutionEvent> {
  private final Query query;
  private final TaskManager taskManager;
  private final EventListener<StageEvent> stageEventListener;
  private final CompletableFuture<QueryResultLocation> resultFuture;
  private final ActorFactory actorFactory;

  /** Only modified by message handler. */
  private final ConcurrentMap<StageId, StageExecution> stageExecutions = new ConcurrentHashMap<>();

  private final Map<StageId, ScheduledStage> scheduledStages = Maps.newHashMap();

  public QueryExecutionActor(
      ActorContext<QueryExecutionEvent> context,
      Query query,
      TaskManager taskManager,
      EventListener<StageEvent> stageEventListener,
      CompletableFuture<QueryResultLocation> resultFuture,
      ActorFactory actorFactory) {
    super(context);
    this.query = requireNonNull(query, "query");
    this.taskManager = requireNonNull(taskManager, "taskManager");
    this.stageEventListener = requireNonNull(stageEventListener, "stageEventListener");
    this.resultFuture = requireNonNull(resultFuture, "resultFuture");
    this.actorFactory = requireNonNull(actorFactory, "actorFactory");
  }

  @Override
  public Receive<QueryExecutionEvent> createReceive() {
    return newReceiveBuilder()
        .onMessage(QueryExecutionEvent.StartEvent.class, this::onStart)
        .onMessage(QueryExecutionEvent.StageStatusChangeEvent.class, this::onStageStatusChanged)
        .build();
  }

  private Behavior<QueryExecutionEvent> onStart(QueryExecutionEvent.StartEvent event) {
    query.getLeafStages().stream()
        .map(this::createOrGetStageExecution)
        .forEach(StageExecution::start);
    return this;
  }

  private Behavior<QueryExecutionEvent> onStageStatusChanged(
      QueryExecutionEvent.StageStatusChangeEvent event) {
    StageEvent stageEvent = event.getStageEvent();

    if (stageEvent instanceof StageEvent.StageScheduledEvent) {
      onStageScheduled((StageEvent.StageScheduledEvent) stageEvent);
    } else {
      throw new PgException(
          PgErrorCode.INTERNAL_ERROR, "Unknown stage event: %s", stageEvent.getClass());
    }

    return this;
  }

  private void onStageScheduled(StageEvent.StageScheduledEvent event) {
    ScheduledStage stage = event.getStageInfo();
    scheduledStages.put(event.getStageId(), stage);

    // Start the stages that have all the dependencies scheduled.
    query.getParentsChecked(event.getStageId()).stream()
        .filter(this::allDependenciesScheduled)
        .map(this::createOrGetStageExecution)
        .forEach(StageExecution::start);

    // Now the entire DAG is scheduled.
    if (event.getStageId().equals(query.getRootStageId())) {
      verify(stage.getAssignments().size() == 1, "Root stage assignment size must be 1");
      Map.Entry<TaskId, WorkerNode> entry = stage.getAssignments().entrySet().iterator().next();

      QueryResultLocation resultLocation =
          new QueryResultLocation(entry.getKey(), entry.getValue());
      resultFuture.complete(resultLocation);
    }
  }

  private StageExecution createOrGetStageExecution(StageId stageId) {
    QueryStage augmentedStage =
        query.getQueryStageChecked(stageId).augmentScheduledInfo(query, getStageChildren(stageId));
    return stageExecutions.computeIfAbsent(
        stageId,
        id ->
            new RemoteStageExecution(
                taskManager, actorFactory, augmentedStage, stageEventListener));
  }

  private ImmutableMap<StageId, ScheduledStage> getStageChildren(StageId stageId) {
    ImmutableSet<StageId> childIds = query.getChildrenChecked(stageId);
    var builder = new ImmutableMap.Builder<StageId, ScheduledStage>();
    childIds.forEach(
        id -> {
          ScheduledStage stage = scheduledStages.get(id);
          requireNonNull(stage, "child must have been scheduled!");
          builder.put(stage.getStageId(), stage);
        });
    return builder.build();
  }

  // Whether all children of the stage has been scheduled.
  private boolean allDependenciesScheduled(StageId stageId) {
    return scheduledStages.keySet().containsAll(query.getChildrenChecked(stageId));
  }
}
