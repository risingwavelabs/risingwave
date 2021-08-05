package com.risingwave.rpc;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Objects.requireNonNull;

import com.risingwave.common.config.Configuration;
import com.risingwave.node.EndPoint;
import com.risingwave.node.WorkerNode;
import com.risingwave.proto.computenode.TaskServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ManagedRpcClientFactory implements RpcClientFactory {
  private final Configuration configuration;

  private final ConcurrentMap<WorkerNode, TaskService> taskServiceClients =
      new ConcurrentHashMap<>();

  @Inject
  public ManagedRpcClientFactory(Configuration configuration) {
    this.configuration = requireNonNull(configuration, "configuration");
  }

  @Override
  public TaskService createTaskServiceClient(WorkerNode node) {
    checkNotNull(node, "node can't be null!");
    return taskServiceClients.computeIfAbsent(
        node,
        n -> {
          // TODO: Use configuration here.
          EndPoint endPoint = n.getRpcEndPoint();
          ManagedChannel channel =
              ManagedChannelBuilder.forAddress(endPoint.getHost(), endPoint.getPort())
                  .usePlaintext()
                  .build();
          return new DefaultTaskService(TaskServiceGrpc.newBlockingStub(channel));
        });
  }
}
