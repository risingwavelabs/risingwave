package com.risingwave.scheduler.streaming.graph;

import com.google.common.collect.ImmutableSet;
import com.risingwave.planner.rel.physical.streaming.RisingWaveStreamingRel;
import com.risingwave.planner.rel.serialization.StreamingStageSerializer;
import com.risingwave.proto.streaming.plan.StreamNode;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The internal representation of a fragment of stream DAG. Each <code>StreamFragment</code> can be
 * translated as one <code>proto.streaming.plan.StreamFragment</code> in proto.
 */
public class StreamFragment {
  private final int id;
  private final StreamDispatcher dispatcher;
  private final RisingWaveStreamingRel root;
  private final ImmutableSet<Integer> upstreamSet;
  private final ImmutableSet<Integer> downstreamSet;

  public StreamFragment(
      int id,
      StreamDispatcher dispatcher,
      RisingWaveStreamingRel root,
      Set<Integer> upstreamSet,
      Set<Integer> downstreamSet) {
    this.id = id;
    this.dispatcher = dispatcher;
    this.root = root;
    this.upstreamSet = ImmutableSet.copyOf(upstreamSet);
    this.downstreamSet = ImmutableSet.copyOf(downstreamSet);
  }

  public int getId() {
    return this.id;
  }

  public RisingWaveStreamingRel getRoot() {
    return this.root;
  }

  public com.risingwave.proto.streaming.plan.StreamFragment serialize() {
    StreamNode node = StreamingStageSerializer.serialize(root);
    com.risingwave.proto.streaming.plan.StreamFragment.Builder builder =
        com.risingwave.proto.streaming.plan.StreamFragment.newBuilder();
    builder.setDispatcher(dispatcher.serialize());
    builder.setNodes(node);
    builder.setFragmentId(id);
    builder.addAllUpstreamFragmentId(upstreamSet);
    builder.addAllDownstreamFragmentId(downstreamSet);
    return builder.build();
  }

  public static FragmentBuilder newBuilder(int id, RisingWaveStreamingRel root) {
    return new FragmentBuilder(id, root);
  }

  public FragmentBuilder toBuilder() {
    FragmentBuilder builder = new FragmentBuilder(this.id, this.root);
    // Add dispatcher to builder.
    if (this.dispatcher != null) {
      switch (this.dispatcher.getDispatcherType()) {
        case SIMPLE:
          builder.addSimpleDispatcher();
          break;
        case ROUND_ROBIN:
          builder.addRoundRobinDispatcher();
          break;
        case HASH:
          builder.addHashDispatcher(this.dispatcher.getDispatcherColumn());
          break;
        case BROADCAST:
          builder.addBroadcastDispatcher();
          break;
        case BLACK_HOLE:
          builder.addBlackHoleDispatcher();
          break;
        default:
          break;
      }
    }
    // Add upstream and downstream.
    for (int id : this.upstreamSet) {
      builder.addUpstream(id);
    }
    for (int id : this.downstreamSet) {
      builder.addDownstream(id);
    }
    return builder;
  }

  /** The builder class for a <code>StreamFragment</code>. */
  public static class FragmentBuilder {
    private int id;
    private RisingWaveStreamingRel root;
    private StreamDispatcher dispatcher = null;
    private final Set<Integer> upstreamSet = new HashSet<>();
    private final Set<Integer> downstreamSet = new HashSet<>();

    public FragmentBuilder(int id, RisingWaveStreamingRel root) {
      this.id = id;
      this.root = root;
    }

    public void addSimpleDispatcher() {
      dispatcher = StreamDispatcher.createSimpleDispatcher();
    }

    public void addRoundRobinDispatcher() {
      dispatcher = StreamDispatcher.createRoundRobinDispatcher();
    }

    public void addHashDispatcher(List<Integer> columns) {
      dispatcher = StreamDispatcher.createHashDispatcher(columns);
    }

    public void addBroadcastDispatcher() {
      dispatcher = StreamDispatcher.createBroadcastDispatcher();
    }

    public void addBlackHoleDispatcher() {
      dispatcher = StreamDispatcher.createBlackHoleDispatcher();
    }

    public void addUpstream(int id) {
      upstreamSet.add(id);
    }

    public void addDownstream(int id) {
      downstreamSet.add(id);
    }

    public StreamFragment build() {
      return new StreamFragment(id, dispatcher, root, upstreamSet, downstreamSet);
    }
  }
}
