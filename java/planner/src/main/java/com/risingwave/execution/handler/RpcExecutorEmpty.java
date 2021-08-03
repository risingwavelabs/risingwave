package com.risingwave.execution.handler;

import com.risingwave.proto.computenode.CreateTaskRequest;
import com.risingwave.proto.computenode.CreateTaskResponse;
import com.risingwave.proto.computenode.TaskData;
import com.risingwave.proto.computenode.TaskSinkId;
import java.util.ArrayList;
import java.util.Iterator;

public class RpcExecutorEmpty implements RpcExecutor {

  public RpcExecutorEmpty() {}

  @Override
  public CreateTaskResponse createTask(CreateTaskRequest request) {
    return CreateTaskResponse.newBuilder().build();
  }

  @Override
  public Iterator<TaskData> getData(TaskSinkId taskSinkId) {
    ArrayList<TaskData> taskData = new ArrayList<TaskData>();
    return taskData.iterator();
  }
}
