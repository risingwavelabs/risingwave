package com.risingwave.rpc;

import com.risingwave.proto.computenode.CreateTaskRequest;
import com.risingwave.proto.computenode.CreateTaskResponse;
import com.risingwave.proto.computenode.TaskData;
import com.risingwave.proto.computenode.TaskSinkId;
import java.util.Iterator;

public interface ComputeClient {
  CreateTaskResponse createTask(CreateTaskRequest request);

  Iterator<TaskData> getData(TaskSinkId taskSinkId);
}
