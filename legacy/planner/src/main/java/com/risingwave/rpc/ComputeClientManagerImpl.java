package com.risingwave.rpc;

import static com.google.common.base.Preconditions.checkNotNull;

import com.risingwave.node.EndPoint;
import com.risingwave.node.WorkerNode;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.inject.Singleton;

@Singleton
public class ComputeClientManagerImpl implements ComputeClientManager {
  private final ConcurrentMap<WorkerNode, ComputeClient> clients = new ConcurrentHashMap<>();

  @Override
  public ComputeClient getOrCreate(WorkerNode node) {
    checkNotNull(node, "node can't be null!");
    return clients.computeIfAbsent(
        node,
        n -> {
          // TODO: Use configuration here.
          EndPoint endPoint = n.getRpcEndPoint();
          ManagedChannel channel =
              ManagedChannelBuilder.forAddress(endPoint.getHost(), endPoint.getPort())
                  .usePlaintext()
                  .build();
          return new GrpcComputeClient(channel);
        });
  }
}
