package com.risingwave.planner.rel.physical.streaming;

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
  public RwStreamAgg(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelNode input,
      ImmutableBitSet groupSet,
      @Nullable List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    super(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls);
    checkConvention();
  }

  @Override
  public StreamNode serialize() {
    StreamNode node;
    if (groupSet.length() == 0) {
      SimpleAggNode.Builder simpleAggNodeBuilder = SimpleAggNode.newBuilder();
      for (AggregateCall aggCall : aggCalls) {
        simpleAggNodeBuilder.addAggCalls(serializeAggCall(aggCall));
      }
      node = StreamNode.newBuilder().setSimpleAggNode(simpleAggNodeBuilder.build()).build();
    } else {
      HashAggNode.Builder hashAggNodeBuilder = HashAggNode.newBuilder();
      for (int i = groupSet.nextSetBit(0); i >= 0; i = groupSet.nextSetBit(i + 1)) {
        hashAggNodeBuilder.addGroupKeys(InputRefExpr.newBuilder().setColumnIdx(i).build());
      }
      for (AggregateCall aggCall : aggCalls) {
        hashAggNodeBuilder.addAggCalls(serializeAggCall(aggCall));
      }
      node = StreamNode.newBuilder().setHashAggNode(hashAggNodeBuilder.build()).build();
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
        getCluster(), traitSet, getHints(), input, groupSet, groupSets, aggCalls);
  }
}
