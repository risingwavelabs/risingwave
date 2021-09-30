package com.risingwave.planner.rel.physical.batch;

import com.google.protobuf.Any;
import com.risingwave.planner.rel.physical.RwAggregate;
import com.risingwave.proto.plan.PlanNode;
import com.risingwave.proto.plan.SimpleAggNode;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Sort agg assumes that its input are sorted according to it group key. */
public class RwBatchSortAgg extends RwAggregate implements RisingWaveBatchPhyRel {
  public RwBatchSortAgg(
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

  private SimpleAggNode serializeSimpleAgg() {
    SimpleAggNode.Builder simpleAggNodeBuilder = SimpleAggNode.newBuilder();
    for (AggregateCall aggCall : aggCalls) {
      simpleAggNodeBuilder.addAggCalls(serializeAggCall(aggCall));
    }
    return simpleAggNodeBuilder.build();
  }

  @Override
  public PlanNode serialize() {
    PlanNode plan;
    if (groupSet.length() == 0) {
      plan =
          PlanNode.newBuilder()
              .setNodeType(PlanNode.PlanNodeType.SIMPLE_AGG)
              .setBody(Any.pack(serializeSimpleAgg()))
              .addChildren(((RisingWaveBatchPhyRel) input).serialize())
              .build();
    } else {
      throw new UnsupportedOperationException();
    }
    return plan;
  }

  @Override
  public Aggregate copy(
      RelTraitSet traitSet,
      RelNode input,
      ImmutableBitSet groupSet,
      @Nullable List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    return new RwBatchSortAgg(getCluster(), traitSet, hints, input, groupSet, groupSets, aggCalls);
  }
}
