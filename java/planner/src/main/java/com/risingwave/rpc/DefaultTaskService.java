package com.risingwave.rpc;

import com.risingwave.proto.computenode.CreateTaskRequest;
import com.risingwave.proto.computenode.CreateTaskResponse;
import com.risingwave.proto.computenode.TaskServiceGrpc;

public class DefaultTaskService implements TaskService {
  private final TaskServiceGrpc.TaskServiceBlockingStub stub;

  public DefaultTaskService(TaskServiceGrpc.TaskServiceBlockingStub stub) {
    this.stub = stub;
  }

  @Override
  public CreateTaskResponse create(CreateTaskRequest request) {
    return stub.create(request);
  }
}
