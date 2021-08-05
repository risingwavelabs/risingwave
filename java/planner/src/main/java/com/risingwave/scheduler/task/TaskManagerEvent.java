package com.risingwave.scheduler.task;

import static java.util.Objects.requireNonNull;

import com.risingwave.scheduler.EventListener;

interface TaskManagerEvent {

  class ScheduleTaskEvent implements TaskManagerEvent {
    private final QueryTask task;
    private final EventListener<TaskEvent> listener;

    ScheduleTaskEvent(QueryTask task, EventListener<TaskEvent> listener) {
      this.task = requireNonNull(task, "task");
      this.listener = requireNonNull(listener, "listener");
    }

    public QueryTask getTask() {
      return task;
    }

    public EventListener<TaskEvent> getListener() {
      return listener;
    }
  }
}
