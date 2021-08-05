package com.risingwave.scheduler.stage;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import com.risingwave.node.WorkerNode;
import com.risingwave.scheduler.task.TaskId;

public interface StageEvent {
  StageId getStageId();

  abstract class StageEventBase implements StageEvent {
    private final StageId stageId;

    protected StageEventBase(StageId stageId) {
      this.stageId = stageId;
    }

    @Override
    public StageId getStageId() {
      return stageId;
    }
  }

  class StageScheduledEvent extends StageEventBase {
    private final ImmutableMap<TaskId, WorkerNode> assignments;

    protected StageScheduledEvent(StageId stageId, ImmutableMap<TaskId, WorkerNode> assignments) {
      super(stageId);
      this.assignments = requireNonNull(assignments, "assignments");
    }

    public ImmutableMap<TaskId, WorkerNode> getAssignments() {
      return assignments;
    }
  }
}
