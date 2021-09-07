package com.risingwave.scheduler.stage;

import akka.actor.typed.ActorRef;
import akka.actor.typed.javadsl.Behaviors;
import com.risingwave.scheduler.EventListener;
import com.risingwave.scheduler.actor.ActorFactory;
import com.risingwave.scheduler.task.TaskEvent;
import com.risingwave.scheduler.task.TaskManager;

public class RemoteStageExecution implements StageExecution, EventListener<TaskEvent> {
  private final ActorRef<StageExecutionEvent> actor;
  private final QueryStage queryStage;

  public RemoteStageExecution(
      TaskManager taskManager,
      ActorFactory actorFactory,
      QueryStage queryStage,
      EventListener<StageEvent> stageEventListener) {
    this.actor =
        actorFactory.createActor(
            Behaviors.setup(
                ctx ->
                    new StageExecutionActor(
                        ctx, taskManager, queryStage, this, stageEventListener)),
            String.format("StageExecutionActor:%s", queryStage.getStageId()));
    this.queryStage = queryStage;
  }

  @Override
  public void start() {
    actor.tell(new StageExecutionEvent.StartEvent(queryStage));
  }

  @Override
  public void onEvent(TaskEvent event) {
    actor.tell(new StageExecutionEvent.TaskStatusChangeEvent(event));
  }
}
