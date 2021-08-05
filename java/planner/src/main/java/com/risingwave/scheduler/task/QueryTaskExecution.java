package com.risingwave.scheduler.task;

import static java.util.Objects.requireNonNull;

import com.risingwave.node.WorkerNode;
import com.risingwave.proto.common.Status;
import com.risingwave.scheduler.EventListener;

class QueryTaskExecution {
  private final QueryTask queryTask;
  private final EventListener<TaskEvent> eventListener;

  QueryTaskExecution(QueryTask queryTask, EventListener<TaskEvent> eventListener) {
    this.queryTask = requireNonNull(queryTask, "queryTask");
    this.eventListener = requireNonNull(eventListener, "eventListener");
  }

  public static QueryTaskExecution from(TaskManagerEvent.ScheduleTaskEvent event) {
    return new QueryTaskExecution(event.getTask(), event.getListener());
  }

  void creationFailed(Status error) {
    eventListener.onEvent(new TaskEvent.TaskCreationFailedEvent(queryTask.getTaskId(), error));
  }

  void taskCreated(WorkerNode node) {
    eventListener.onEvent(new TaskEvent.TaskCreatedEvent(queryTask.getTaskId(), node));
  }
}
