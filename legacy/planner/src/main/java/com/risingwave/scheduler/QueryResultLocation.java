package com.risingwave.scheduler;

import static java.util.Objects.requireNonNull;

import com.risingwave.node.WorkerNode;
import com.risingwave.scheduler.task.TaskId;

/** Where the query root (gather node) is assigned. */
public class QueryResultLocation {
  private final TaskId taskId;
  private final WorkerNode node;

  public QueryResultLocation(TaskId taskId, WorkerNode node) {
    this.taskId = requireNonNull(taskId, "taskId");
    this.node = requireNonNull(node, "node");
  }

  public TaskId getTaskId() {
    return taskId;
  }

  public WorkerNode getNode() {
    return node;
  }
}
