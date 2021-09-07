package com.risingwave.scheduler.stage;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Receive;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.risingwave.node.WorkerNode;
import com.risingwave.scheduler.EventListener;
import com.risingwave.scheduler.task.QueryTask;
import com.risingwave.scheduler.task.TaskEvent;
import com.risingwave.scheduler.task.TaskId;
import com.risingwave.scheduler.task.TaskManager;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

class StageExecutionActor extends AbstractBehavior<StageExecutionEvent> {
  private final TaskManager taskManager;
  private final QueryStage stage;
  private final EventListener<TaskEvent> taskEventListener;
  private final EventListener<StageEvent> stageEventListener;

  private final Map<TaskId, WorkerNode> scheduleTasks;

  public StageExecutionActor(
      ActorContext<StageExecutionEvent> context,
      TaskManager taskManager,
      QueryStage stage,
      EventListener<TaskEvent> taskEventListener,
      EventListener<StageEvent> stageEventListener) {
    super(context);
    this.taskManager = taskManager;
    this.stage = stage;
    this.taskEventListener = taskEventListener;

    this.scheduleTasks = new HashMap<>(stage.getParallelism());
    this.stageEventListener = stageEventListener;
  }

  @Override
  public Receive<StageExecutionEvent> createReceive() {
    return newReceiveBuilder()
        .onMessage(StageExecutionEvent.StartEvent.class, this::onStart)
        .onMessage(StageExecutionEvent.TaskStatusChangeEvent.class, this::onTaskStatusChange)
        .build();
  }

  private Behavior<StageExecutionEvent> onStart(StageExecutionEvent.StartEvent event) {
    QueryStage stage = event.getQueryStage();

    ImmutableList<QueryTask> tasks =
        IntStream.range(0, stage.getParallelism())
            .mapToObj(idx -> new QueryTask(new TaskId(stage.getStageId(), idx), stage))
            .collect(ImmutableList.toImmutableList());
    tasks.forEach(task -> taskManager.schedule(task, taskEventListener));
    return this;
  }

  private Behavior<StageExecutionEvent> onTaskStatusChange(
      StageExecutionEvent.TaskStatusChangeEvent event) {

    TaskEvent taskEvent = event.getTaskEvent();
    if (taskEvent instanceof TaskEvent.TaskCreatedEvent) {
      onTaskCreated((TaskEvent.TaskCreatedEvent) taskEvent);
    } else {
      getContext().getLog().error("Unknown task event type: {}", taskEvent.getClass());
    }
    return this;
  }

  private void onTaskCreated(TaskEvent.TaskCreatedEvent taskCreatedEvent) {
    getContext().getLog().info("Query execution task created: {}", taskCreatedEvent.getTaskId());

    scheduleTasks.put(taskCreatedEvent.getTaskId(), taskCreatedEvent.getNode());

    if (scheduleTasks.size() == stage.getParallelism()) {
      getContext().getLog().info("All tasks in stage {} scheduled.", stage.getStageId());

      var scheduledInfo = new ScheduledStage(stage, ImmutableMap.copyOf(scheduleTasks));
      stageEventListener.onEvent(
          new StageEvent.StageScheduledEvent(stage.getStageId(), scheduledInfo));
    }
  }
}
