package com.risingwave.rpc;

import com.risingwave.node.WorkerNode;
import com.risingwave.proto.common.Status;
import com.risingwave.proto.computenode.CreateTaskRequest;
import com.risingwave.proto.computenode.CreateTaskResponse;
import javax.inject.Singleton;

@Singleton
public class TestRpcClientFactory implements RpcClientFactory {
  @Override
  public TaskService createTaskServiceClient(WorkerNode node) {
    return new TaskService() {
      @Override
      public CreateTaskResponse create(CreateTaskRequest request) {
        return CreateTaskResponse.newBuilder()
            .setStatus(Status.newBuilder().setCode(Status.Code.OK))
            .build();
      }
    };
  }
}
