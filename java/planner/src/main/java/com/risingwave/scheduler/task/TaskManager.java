package com.risingwave.scheduler.task;

import com.risingwave.scheduler.EventListener;

public interface TaskManager {
  void schedule(QueryTask task, EventListener<TaskEvent> taskEventListener);
}
