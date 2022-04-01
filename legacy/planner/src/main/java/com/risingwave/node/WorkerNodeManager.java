package com.risingwave.node;

import java.util.List;

public interface WorkerNodeManager {
  WorkerNode nextRandom();

  List<WorkerNode> allNodes();
}
