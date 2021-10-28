package com.risingwave.planner.rel.physical.batch;

import com.google.protobuf.Any;
import com.risingwave.planner.rel.physical.RwAggregate;
import com.risingwave.proto.plan.HashAggNode;
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

/** Plan for adhoc HashAgg executor. */
public class RwBatchHashAgg extends RwAggregate implements RisingWaveBatchPhyRel {
  public RwBatchHashAgg(
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

  private HashAggNode serializeHashAgg() {
    HashAggNode.Builder hashAggNodeBuilder = HashAggNode.newBuilder();
    for (int i = groupSet.nextSetBit(0); i >= 0; i = groupSet.nextSetBit(i + 1)) {
      hashAggNodeBuilder.addGroupKeys(i);
    }
    for (AggregateCall aggCall : aggCalls) {
      hashAggNodeBuilder.addAggCalls(serializeAggCall(aggCall));
    }
    return hashAggNodeBuilder.build();
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
      plan =
          PlanNode.newBuilder()
              .setNodeType(PlanNode.PlanNodeType.HASH_AGG)
              .setBody(Any.pack(serializeHashAgg()))
              .addChildren(((RisingWaveBatchPhyRel) input).serialize())
              .build();
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
    return new RwBatchHashAgg(
        getCluster(), traitSet, getHints(), input, groupSet, getGroupSets(), aggCalls);
  }
}
