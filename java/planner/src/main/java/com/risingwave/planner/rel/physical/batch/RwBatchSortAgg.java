package com.risingwave.planner.rel.physical.batch;

import com.google.protobuf.Any;
import com.risingwave.planner.rel.physical.RwAggregate;
import com.risingwave.planner.rel.serialization.RexToProtoSerializer;
import com.risingwave.proto.plan.PlanNode;
import com.risingwave.proto.plan.SortAggNode;
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

  private SortAggNode serializeSortAgg() {
    SortAggNode.Builder sortAggNodeBuilder = SortAggNode.newBuilder();
    for (AggregateCall aggCall : aggCalls) {
      sortAggNodeBuilder.addAggCalls(serializeAggCall(aggCall));
    }
    for (int i = groupSet.nextSetBit(0); i >= 0; i = groupSet.nextSetBit(i + 1)) {
      sortAggNodeBuilder.addGroupKeys(
          getCluster().getRexBuilder().makeInputRef(input, i).accept(new RexToProtoSerializer()));
    }
    return sortAggNodeBuilder.build();
  }

  @Override
  public PlanNode serialize() {
    return PlanNode.newBuilder()
        .setNodeType(PlanNode.PlanNodeType.SORT_AGG)
        .setBody(Any.pack(serializeSortAgg()))
        .addChildren(((RisingWaveBatchPhyRel) input).serialize())
        .build();
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
