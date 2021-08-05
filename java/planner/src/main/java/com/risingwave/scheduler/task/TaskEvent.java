package com.risingwave.scheduler.task;

import static java.util.Objects.requireNonNull;

import com.risingwave.node.WorkerNode;
import com.risingwave.proto.common.Status;

public interface TaskEvent {
  TaskId getTaskId();

  abstract class TaskEventBase implements TaskEvent {
    private final TaskId taskId;

    protected TaskEventBase(TaskId taskId) {
      this.taskId = taskId;
    }

    @Override
    public TaskId getTaskId() {
      return taskId;
    }
  }

  class TaskCreationFailedEvent extends TaskEventBase {
    private final Status status;

    public TaskCreationFailedEvent(TaskId taskId, Status status) {
      super(taskId);
      this.status = status;
    }

    public Status getStatus() {
      return status;
    }
  }

  class TaskCreatedEvent extends TaskEventBase {
    private final WorkerNode node;

    public TaskCreatedEvent(TaskId taskId, WorkerNode node) {
      super(taskId);
      this.node = requireNonNull(node, "node");
    }

    public WorkerNode getNode() {
      return node;
    }
  }
}
