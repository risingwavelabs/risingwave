package com.risingwave.scheduler.streaming;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.risingwave.node.WorkerNode;
import com.risingwave.proto.common.HostAddress;
import com.risingwave.proto.streaming.streamnode.ActorInfo;
import com.risingwave.proto.streaming.streamnode.BroadcastActorInfoTableRequest;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Actor info table to be serialized as proto. */
public class ActorInfoTable {
  private final ImmutableMap<WorkerNode, ImmutableSet<Integer>> actorInfoTable;

  public ActorInfoTable(Map<WorkerNode, Set<Integer>> actorInfoTable) {
    this.actorInfoTable = immutableActorMap(actorInfoTable);
  }

  private static ImmutableMap<WorkerNode, ImmutableSet<Integer>> immutableActorMap(
      Map<WorkerNode, Set<Integer>> map) {
    var builder = new ImmutableMap.Builder<WorkerNode, ImmutableSet<Integer>>();
    map.forEach((node, set) -> builder.put(node, ImmutableSet.copyOf(set)));
    return builder.build();
  }

  public BroadcastActorInfoTableRequest serialize() {
    BroadcastActorInfoTableRequest.Builder builder = BroadcastActorInfoTableRequest.newBuilder();
    for (WorkerNode node : actorInfoTable.keySet()) {
      var fragmentIds = actorInfoTable.get(node);
      HostAddress.Builder addressBuilder = HostAddress.newBuilder();
      addressBuilder.setHost(node.getRpcEndPoint().getHost());
      addressBuilder.setPort(node.getRpcEndPoint().getPort());
      var hostAddress = addressBuilder.build();
      for (Integer fragmentId : fragmentIds) {
        ActorInfo.Builder actorBuilder = ActorInfo.newBuilder();
        actorBuilder.setFragmentId(fragmentId);
        actorBuilder.setHost(hostAddress);
        builder.addInfo(actorBuilder.build());
      }
    }
    return builder.build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /** The builder class of a <code>ActorInfoTable</code>. */
  public static class Builder {
    private final Map<WorkerNode, Set<Integer>> actorInfoTable = new HashMap<>();

    public Builder() {}

    public void addWorkerNode(WorkerNode node, Set<Integer> fragmentIds) {
      actorInfoTable.put(node, fragmentIds);
    }

    public ActorInfoTable build() {
      return new ActorInfoTable(actorInfoTable);
    }
  }
}
