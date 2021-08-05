package com.risingwave.scheduler.stage;

import com.risingwave.scheduler.task.TaskEvent;

public interface StageExecutionEvent {
  class StartEvent implements StageExecutionEvent {}

  class TaskStatusChangeEvent implements StageExecutionEvent {
    private final TaskEvent taskEvent;

    public TaskStatusChangeEvent(TaskEvent taskEvent) {
      this.taskEvent = taskEvent;
    }

    public TaskEvent getTaskEvent() {
      return taskEvent;
    }
  }
}
