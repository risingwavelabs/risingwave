package com.risingwave.scheduler.query;

import static java.util.Objects.requireNonNull;

import akka.actor.typed.ActorRef;
import akka.actor.typed.javadsl.Behaviors;
import com.risingwave.scheduler.EventListener;
import com.risingwave.scheduler.QueryResultLocation;
import com.risingwave.scheduler.ResourceManager;
import com.risingwave.scheduler.actor.ActorFactory;
import com.risingwave.scheduler.stage.StageEvent;
import com.risingwave.scheduler.task.TaskManager;
import java.util.concurrent.CompletableFuture;

public class RemoteQueryExecution implements QueryExecution, EventListener<StageEvent> {
  private final Query query;
  private final ActorRef<QueryExecutionEvent> actor;

  public RemoteQueryExecution(
      Query query,
      ActorFactory actorFactory,
      TaskManager taskManager,
      ResourceManager resourceManager,
      CompletableFuture<QueryResultLocation> resultFuture) {
    this.query = requireNonNull(query, "query");
    requireNonNull(resultFuture, "notifier");

    this.actor =
        actorFactory.createActor(
            Behaviors.setup(
                ctx ->
                    new QueryExecutionActor(
                        ctx,
                        query,
                        taskManager,
                        this,
                        resultFuture,
                        actorFactory,
                        resourceManager)),
            String.format("QueryExecutionActor:%s", query.getQueryId()));
  }

  @Override
  public void start() {
    actor.tell(new QueryExecutionEvent.StartEvent());
  }

  @Override
  public void onEvent(StageEvent event) {
    actor.tell(new QueryExecutionEvent.StageStatusChangeEvent(event));
  }
}
