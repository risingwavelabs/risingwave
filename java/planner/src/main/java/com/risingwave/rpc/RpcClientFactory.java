package com.risingwave.rpc;

import com.risingwave.node.WorkerNode;

public interface RpcClientFactory {
  TaskService createTaskServiceClient(WorkerNode node);
}
