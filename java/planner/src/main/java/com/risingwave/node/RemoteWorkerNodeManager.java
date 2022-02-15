package com.risingwave.node;

import com.google.common.collect.ImmutableList;
import com.risingwave.common.config.Configuration;
import com.risingwave.common.config.FrontendServerConfigurations;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.proto.common.Status;
import com.risingwave.proto.common.WorkerType;
import com.risingwave.proto.metanode.ListAllNodesRequest;
import com.risingwave.proto.metanode.ListAllNodesResponse;
import com.risingwave.rpc.MetaClient;
import com.risingwave.rpc.MetaClientManager;
import java.util.Random;
import javax.inject.Inject;

/**
 * An implementation of worker node manager by sync with meta service.
 *
 * <p>Note: 2021.12.13: copied from LocalWorkerNodeManager for now.
 */
public class RemoteWorkerNodeManager implements WorkerNodeManager {
  private final ImmutableList<WorkerNode> workerNodes;
  private final Random random = new Random(1024);

  @Inject
  public RemoteWorkerNodeManager(Configuration conf, MetaClientManager metaClientManager) {
    String address = conf.get(FrontendServerConfigurations.META_SERVICE_ADDRESS);
    DefaultWorkerNode node = DefaultWorkerNode.from(address);
    MetaClient metaClient =
        metaClientManager.getOrCreate(
            node.getRpcEndPoint().getHost(), node.getRpcEndPoint().getPort());

    ListAllNodesRequest request =
        ListAllNodesRequest.newBuilder().setWorkerType(WorkerType.COMPUTE_NODE).build();
    ListAllNodesResponse response = metaClient.listAllNodes(request);
    if (response.getStatus().getCode() != Status.Code.OK) {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, "list all nodes failed");
    }

    workerNodes =
        response.getNodesList().stream()
            .map((e) -> String.format("%s:%d", e.getHost().getHost(), e.getHost().getPort()))
            .map(DefaultWorkerNode::from)
            .collect(ImmutableList.toImmutableList());
  }

  @Override
  public WorkerNode nextRandom() {
    return workerNodes.get(random.nextInt(workerNodes.size()));
  }

  @Override
  public ImmutableList<WorkerNode> allNodes() {
    return workerNodes;
  }
}
