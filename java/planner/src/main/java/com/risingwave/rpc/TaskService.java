package com.risingwave.rpc;

import com.risingwave.proto.computenode.CreateTaskRequest;
import com.risingwave.proto.computenode.CreateTaskResponse;

public interface TaskService {
  CreateTaskResponse create(CreateTaskRequest request);
}
