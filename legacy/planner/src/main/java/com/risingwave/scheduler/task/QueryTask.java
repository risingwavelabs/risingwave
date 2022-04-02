package com.risingwave.scheduler.task;

import static java.util.Objects.requireNonNull;

import com.risingwave.scheduler.stage.QueryStage;

/** The QueryTask is scheduled by TaskManager. */
public class QueryTask {
  private final TaskId taskId;
  private final QueryStage stage;
  private final long epoch;

  public QueryTask(TaskId taskId, QueryStage stage, long epoch) {
    this.taskId = requireNonNull(taskId, "taskId");
    this.stage = requireNonNull(stage, "stage");
    this.epoch = epoch;
  }

  public TaskId getTaskId() {
    return taskId;
  }

  public QueryStage getQueryStage() {
    return stage;
  }

  public long getEpoch() {
    return epoch;
  }
}
