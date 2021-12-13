package com.risingwave.node;

import static com.risingwave.common.config.LeaderServerConfigurations.COMPUTE_NODES;

import com.google.common.collect.ImmutableList;
import com.risingwave.common.config.Configuration;
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
  public RemoteWorkerNodeManager(Configuration conf) {
    workerNodes =
        conf.get(COMPUTE_NODES).stream()
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
