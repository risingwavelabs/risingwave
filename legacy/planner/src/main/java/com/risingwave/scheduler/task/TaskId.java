package com.risingwave.scheduler.task;

import static java.util.Objects.requireNonNull;

import com.google.common.base.Objects;
import com.risingwave.scheduler.stage.StageId;

/** The id of Task in a stage */
public class TaskId {
  private final StageId stageId;
  private final int id;

  public TaskId(StageId stageId, int id) {
    this.stageId = requireNonNull(stageId, "stageId");
    this.id = id;
  }

  public StageId getStageId() {
    return stageId;
  }

  public int getId() {
    return id;
  }

  public com.risingwave.proto.plan.TaskId toTaskIdProto() {
    return com.risingwave.proto.plan.TaskId.newBuilder()
        .setQueryId(stageId.getQueryId().getId())
        .setStageId(stageId.getId())
        .setTaskId(id)
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TaskId taskId = (TaskId) o;
    return id == taskId.id && Objects.equal(stageId, taskId.stageId);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(stageId, id);
  }

  @Override
  public String toString() {
    return stageId + "." + id;
  }
}
