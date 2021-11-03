package com.risingwave.rpc;

import com.risingwave.proto.computenode.CreateTaskRequest;
import com.risingwave.proto.computenode.CreateTaskResponse;
import com.risingwave.proto.computenode.GetDataResponse;
import com.risingwave.proto.computenode.TaskSinkId;
import java.util.Iterator;

/** A client connecting to ComputeNodes. */
public interface ComputeClient {
  CreateTaskResponse createTask(CreateTaskRequest request);

  Iterator<GetDataResponse> getData(TaskSinkId taskSinkId);
}
