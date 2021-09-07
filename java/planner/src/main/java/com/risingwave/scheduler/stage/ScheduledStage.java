package com.risingwave.scheduler.stage;

import com.google.common.collect.ImmutableMap;
import com.risingwave.node.WorkerNode;
import com.risingwave.scheduler.task.TaskId;

/** Full information of scheduled stage. */
public class ScheduledStage {
  private final QueryStage stage;
  private final ImmutableMap<TaskId, WorkerNode> assignments;

  public ScheduledStage(QueryStage stage, ImmutableMap<TaskId, WorkerNode> assignments) {
    this.stage = stage;
    this.assignments = assignments;
  }

  public StageId getStageId() {
    return stage.getStageId();
  }

  public ImmutableMap<TaskId, WorkerNode> getAssignments() {
    return assignments;
  }
}
