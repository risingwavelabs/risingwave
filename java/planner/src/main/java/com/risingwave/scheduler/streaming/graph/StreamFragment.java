package com.risingwave.scheduler.streaming.graph;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.risingwave.catalog.ColumnDesc;
import com.risingwave.planner.rel.physical.streaming.RisingWaveStreamingRel;
import com.risingwave.planner.rel.serialization.StreamingStageSerializer;
import com.risingwave.proto.streaming.plan.Merger;
import com.risingwave.proto.streaming.plan.StreamNode;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.util.Pair;

/**
 * The internal representation of a fragment of stream DAG. Each <code>StreamFragment</code> can be
 * translated as one <code>proto.streaming.plan.StreamFragment</code> in proto.
 */
public class StreamFragment {
  private final int id;
  private final StreamDispatcher dispatcher;
  private final RisingWaveStreamingRel root;
  // For each pair in the upstreamSets:
  // The first element of pair is upstream stage id.
  // The second element of pair is a set of upstream fragment ids that belong to the same stage.
  private final ImmutableList<Pair<Integer, ImmutableSet<Integer>>> upstreamSets;
  private final ImmutableSet<Integer> downstreamSet;
  // A temporary solution for input schema of stream fragment.
  // Assuming there is no join, a schema is a list of columns.
  private final ImmutableList<ColumnDesc> inputSchema;

  public StreamFragment(
      int id,
      StreamDispatcher dispatcher,
      RisingWaveStreamingRel root,
      List<Pair<Integer, Set<Integer>>> upstreamSets,
      Set<Integer> downstreamSet,
      List<ColumnDesc> inputSchema) {
    this.id = id;
    this.dispatcher = dispatcher;
    this.root = root;
    List<Pair<Integer, ImmutableSet<Integer>>> upStreamImmutableSets = new ArrayList<>();
    for (var p : upstreamSets) {
      upStreamImmutableSets.add(new Pair<>(p.left, ImmutableSet.copyOf(p.right)));
    }
    this.upstreamSets = ImmutableList.copyOf(upStreamImmutableSets);
    this.downstreamSet = ImmutableSet.copyOf(downstreamSet);
    this.inputSchema = ImmutableList.copyOf(inputSchema);
  }

  public int getId() {
    return this.id;
  }

  public ImmutableList<Pair<Integer, ImmutableSet<Integer>>> getUpstreamSets() {
    return this.upstreamSets;
  }

  public ImmutableSet<Integer> getDownstreamSet() {
    return this.downstreamSet;
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
    for (var p : upstreamSets) {
      Merger.Builder mergerBuilder = Merger.newBuilder();
      mergerBuilder.addAllUpstreamFragmentId(p.right);
      builder.addMergers(mergerBuilder.build());
    }
    builder.addAllDownstreamFragmentId(downstreamSet);
    // Also serialize input schema.
    // TODO: move proto.plan.ColumnDesc serialization to catalog.ColumnDesc.
    for (ColumnDesc columnDesc : inputSchema) {
      com.risingwave.proto.plan.ColumnDesc.Builder columnDescBuilder =
          com.risingwave.proto.plan.ColumnDesc.newBuilder();
      columnDescBuilder
          .setEncoding(com.risingwave.proto.plan.ColumnDesc.ColumnEncodingType.RAW)
          .setColumnType(columnDesc.getDataType().getProtobufType())
          .setIsPrimary(columnDesc.isPrimary());
      builder.addInputColumnDescs(columnDescBuilder.build());
    }
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
    for (var merger : this.upstreamSets) {
      var upstreamStageId = merger.left;
      for (var upstreamFragmentId : merger.right) {
        builder.addUpstream(upstreamStageId, upstreamFragmentId);
      }
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
    private final List<Pair<Integer, Set<Integer>>> upstreamSets = new ArrayList<>();
    private final Set<Integer> downstreamSet = new HashSet<>();
    private final List<ColumnDesc> inputSchema = new ArrayList<>();

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

    public void addUpstream(int upstreamStageId, int upstreamFragmentId) {
      for (int idx = 0; idx < upstreamSets.size(); idx++) {
        var stageId = upstreamSets.get(idx).left;
        if (upstreamStageId == stageId) {
          upstreamSets.get(idx).right.add(upstreamFragmentId);
          return;
        }
      }
      upstreamSets.add(new Pair<>(upstreamStageId, new HashSet<>()));
      upstreamSets.get(upstreamSets.size() - 1).right.add(upstreamFragmentId);
    }

    public void addDownstream(int id) {
      downstreamSet.add(id);
    }

    public void addInputSchema(List<ColumnDesc> schema) {
      inputSchema.clear();
      inputSchema.addAll(schema);
    }

    public StreamFragment build() {
      return new StreamFragment(id, dispatcher, root, upstreamSets, downstreamSet, inputSchema);
    }
  }
}
