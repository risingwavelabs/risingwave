package com.risingwave.scheduler.stage;

import akka.actor.typed.ActorRef;
import akka.actor.typed.javadsl.Behaviors;
import com.risingwave.scheduler.EventListener;
import com.risingwave.scheduler.actor.ActorFactory;
import com.risingwave.scheduler.task.TaskEvent;
import com.risingwave.scheduler.task.TaskManager;

public class RemoteStageExecution implements StageExecution, EventListener<TaskEvent> {
  private final TaskManager taskManager;
  private final QueryStage queryStage;
  private final ActorRef<StageExecutionEvent> actor;
  private final EventListener<StageEvent> stageEventListener;

  public RemoteStageExecution(
      TaskManager taskManager,
      ActorFactory actorFactory,
      QueryStage queryStage,
      EventListener<StageEvent> stageEventListener) {
    this.taskManager = taskManager;
    this.queryStage = queryStage;
    this.stageEventListener = stageEventListener;
    this.actor =
        actorFactory.createActor(
            Behaviors.setup(
                ctx ->
                    new StageExecutionActor(
                        ctx, taskManager, queryStage, this, stageEventListener)),
            String.format("StageExecutionActor:%s", queryStage.getStageId()));
  }

  @Override
  public void start() {
    actor.tell(new StageExecutionEvent.StartEvent());
  }

  @Override
  public void onEvent(TaskEvent event) {
    actor.tell(new StageExecutionEvent.TaskStatusChangeEvent(event));
  }
}
