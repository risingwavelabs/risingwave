package com.risingwave.planner.rel.physical.streaming;

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

public class RwStreamGlobalAgg extends Aggregate implements RisingWaveStreamingRel {
  public RwStreamGlobalAgg(
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
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Aggregate copy(
      RelTraitSet traitSet,
      RelNode input,
      ImmutableBitSet groupSet,
      @Nullable List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    return new RwStreamGlobalAgg(
        getCluster(), traitSet, getHints(), input, groupSet, groupSets, aggCalls);
  }
}
