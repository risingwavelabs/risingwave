package com.risingwave.execution.context;

import com.google.inject.Inject;
import com.risingwave.common.config.Configuration;
import com.risingwave.node.WorkerNodeManager;

public class LeaderEnv {
  private final WorkerNodeManager nodeManager;
  private final Configuration conf;

  @Inject
  public LeaderEnv(WorkerNodeManager nodeManager, Configuration conf) {
    this.nodeManager = nodeManager;
    this.conf = conf;
  }

  public WorkerNodeManager getWorkerNodeManager() {
    return nodeManager;
  }

  public Configuration getConfiguration() {
    return conf;
  }
}
