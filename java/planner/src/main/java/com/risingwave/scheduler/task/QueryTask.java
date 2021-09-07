package com.risingwave.scheduler.task;

import static java.util.Objects.requireNonNull;

import com.risingwave.scheduler.stage.QueryStage;

public class QueryTask {
  private final TaskId taskId;
  private final QueryStage stage;

  public QueryTask(TaskId taskId, QueryStage stage) {
    this.taskId = requireNonNull(taskId, "taskId");
    this.stage = requireNonNull(stage, "stage");
  }

  public TaskId getTaskId() {
    return taskId;
  }

  public QueryStage getQueryStage() {
    return stage;
  }
}
