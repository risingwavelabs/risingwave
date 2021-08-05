package com.risingwave.scheduler;

import static java.util.Objects.requireNonNull;

import com.risingwave.node.EndPoint;
import com.risingwave.scheduler.task.TaskId;

public class QueryResultLocation {
  private final TaskId taskId;
  private final EndPoint nodeEndPoint;

  public QueryResultLocation(TaskId taskId, EndPoint nodeEndPoint) {
    this.taskId = requireNonNull(taskId, "taskId");
    this.nodeEndPoint = requireNonNull(nodeEndPoint, "nodeEndPoint");
  }

  public TaskId getTaskId() {
    return taskId;
  }

  public EndPoint getNodeEndPoint() {
    return nodeEndPoint;
  }
}
