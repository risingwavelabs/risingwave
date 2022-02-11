package com.risingwave.planner.rel.streaming;

import com.risingwave.planner.metadata.RisingWaveRelMetadataQuery;
import com.risingwave.planner.rel.physical.RwAggregate;
import com.risingwave.proto.expr.InputRefExpr;
import com.risingwave.proto.streaming.plan.HashAggNode;
import com.risingwave.proto.streaming.plan.SimpleAggNode;
import com.risingwave.proto.streaming.plan.StreamNode;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Stream Aggregation */
public class RwStreamAgg extends RwAggregate implements RisingWaveStreamingRel {
  boolean isGlobal;

  public RwStreamAgg(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelNode input,
      ImmutableBitSet groupSet,
      @Nullable List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    this(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls, true);
  }

  public RwStreamAgg(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelNode input,
      ImmutableBitSet groupSet,
      @Nullable List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls,
      boolean isGlobal) {
    super(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls);
    this.isGlobal = isGlobal;
    checkConvention();
  }

  @Override
  public StreamNode serialize() {
    StreamNode node;
    var primaryKeyIndices =
        ((RisingWaveRelMetadataQuery) getCluster().getMetadataQuery()).getPrimaryKeyIndices(this);
    if (groupSet.length() == 0) {
      SimpleAggNode.Builder simpleAggNodeBuilder = SimpleAggNode.newBuilder();
      for (AggregateCall aggCall : aggCalls) {
        simpleAggNodeBuilder.addAggCalls(serializeAggCall(aggCall));
      }
      var simpleAggNode = simpleAggNodeBuilder.build();
      var builder = StreamNode.newBuilder();
      if (isGlobal) {
        builder.setSimpleAggNode(simpleAggNode);
      } else {
        builder.setLocalSimpleAggNode(simpleAggNode);
      }
      builder.addAllPkIndices(primaryKeyIndices);
      node = builder.build();
    } else {
      HashAggNode.Builder hashAggNodeBuilder = HashAggNode.newBuilder();
      for (int i = groupSet.nextSetBit(0); i >= 0; i = groupSet.nextSetBit(i + 1)) {
        hashAggNodeBuilder.addGroupKeys(InputRefExpr.newBuilder().setColumnIdx(i).build());
      }
      for (AggregateCall aggCall : aggCalls) {
        hashAggNodeBuilder.addAggCalls(serializeAggCall(aggCall));
      }

      node =
          StreamNode.newBuilder()
              .setHashAggNode(hashAggNodeBuilder.build())
              .addAllPkIndices(primaryKeyIndices)
              .build();
    }
    return node;
  }

  @Override
  public Aggregate copy(
      RelTraitSet traitSet,
      RelNode input,
      ImmutableBitSet groupSet,
      @Nullable List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    return new RwStreamAgg(
        getCluster(), traitSet, getHints(), input, groupSet, groupSets, aggCalls, isGlobal);
  }

  @Override
  public <T> RwStreamingRelVisitor.Result<T> accept(RwStreamingRelVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
