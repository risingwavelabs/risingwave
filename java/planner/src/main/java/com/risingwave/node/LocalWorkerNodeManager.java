package com.risingwave.node;

import static com.risingwave.common.config.LeaderServerConfigurations.COMPUTE_NODES;

import com.google.common.collect.ImmutableList;
import com.risingwave.common.config.Configuration;
import java.util.Random;
import javax.inject.Inject;
import javax.inject.Singleton;

/** A simple worker node manager that loads worker nodes from config. */
@Singleton
public class LocalWorkerNodeManager implements WorkerNodeManager {
  private final ImmutableList<WorkerNode> workerNodes;
  private final Random random = new Random(1024);

  @Inject
  public LocalWorkerNodeManager(Configuration conf) {
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
