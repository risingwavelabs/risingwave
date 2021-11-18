package com.risingwave.scheduler.streaming;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.node.WorkerNode;
import com.risingwave.node.WorkerNodeManager;
import com.risingwave.scheduler.streaming.graph.StreamFragment;
import com.risingwave.scheduler.streaming.graph.StreamGraph;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** The default implementation of the interface <code>StreamManager</code>. */
@Singleton
public class StreamManagerImpl implements StreamManager {
  private int fragmentId = 1;
  private int scheduleId = 0;
  private final WorkerNodeManager workerNodeManager;
  private final Map<WorkerNode, Set<StreamFragment>> fragmentAllocation = new HashMap<>();

  @Inject
  public StreamManagerImpl(WorkerNodeManager workerNodeManager) {
    this.workerNodeManager = workerNodeManager;
  }

  @Override
  public int createFragment() {
    return fragmentId++;
  }

  @Override
  public List<StreamRequest> scheduleStreamGraph(StreamGraph graph) {
    List<StreamFragment> fragmentList = graph.getAllFragments();

    List<WorkerNode> nodeList = workerNodeManager.allNodes();
    if (nodeList.size() != 1) {
      throw new PgException(
          PgErrorCode.INTERNAL_ERROR, "Support only one worker node for streaming now.");
    }
    // An ugly scheduling algorithm for 1 worker: put everything on the node0.
    WorkerNode node0 = nodeList.get(0);
    for (var fragment : fragmentList) {
      addFragmentToWorker(node0, fragment);
    }

    // Build returning list.
    List<StreamRequest> requestList = new ArrayList<>();
    for (WorkerNode node : fragmentAllocation.keySet()) {
      StreamRequest.Builder builder = StreamRequest.newBuilder();
      builder.setWorkerNode(node);
      for (var fragment : fragmentAllocation.get(node)) {
        builder.addStreamFragment(fragment);
      }
      requestList.add(builder.build());
    }
    return requestList;
  }

  private void addFragmentToWorker(WorkerNode node, StreamFragment fragment) {
    if (fragmentAllocation.containsKey(node)) {
      fragmentAllocation.get(node).add(fragment);
    } else {
      Set<StreamFragment> fragmentSet = new HashSet<>();
      fragmentSet.add(fragment);
      fragmentAllocation.put(node, fragmentSet);
    }
  }

  @Override
  public int nextScheduleId() {
    return scheduleId++;
  }

  @Override
  public ActorInfoTable getActorInfo() {
    ActorInfoTable.Builder builder = ActorInfoTable.newBuilder();
    for (var node : fragmentAllocation.keySet()) {
      Set<Integer> fragmentIdSet = new HashSet<Integer>();
      for (var fragment : fragmentAllocation.get(node)) {
        fragmentIdSet.add(fragment.getId());
      }
      builder.addWorkerNode(node, fragmentIdSet);
    }
    return builder.build();
  }
}
