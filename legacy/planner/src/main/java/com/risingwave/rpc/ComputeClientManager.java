package com.risingwave.rpc;

import com.risingwave.node.WorkerNode;

/**
 * This class manages a pool of RPC clients.<br>
 * Implementation should be able to reuse the channel if exists.
 */
public interface ComputeClientManager {
  ComputeClient getOrCreate(WorkerNode node);
}
