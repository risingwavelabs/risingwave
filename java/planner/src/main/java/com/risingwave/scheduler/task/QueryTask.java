package com.risingwave.scheduler.task;

import static java.util.Objects.requireNonNull;

import com.risingwave.scheduler.stage.StagePlanInfo;

public class QueryTask {
  private final TaskId taskId;
  private final StagePlanInfo stagePlanInfo;

  public QueryTask(TaskId taskId, StagePlanInfo stagePlanInfo) {
    this.taskId = requireNonNull(taskId, "taskId");
    this.stagePlanInfo = requireNonNull(stagePlanInfo, "planFragment");
  }

  public TaskId getTaskId() {
    return taskId;
  }

  public StagePlanInfo getPlanFragment() {
    return stagePlanInfo;
  }
}
