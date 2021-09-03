package com.risingwave.planner.rel.physical.batch;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.protobuf.Any;
import com.risingwave.planner.rel.serialization.RexToProtoSerializer;
import com.risingwave.proto.expr.ExprNode;
import com.risingwave.proto.plan.PlanNode;
import com.risingwave.proto.plan.SimpleAggNode;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Sort agg assumes that its input are sorted according to it group key. */
public class RwBatchSortAgg extends Aggregate implements RisingWaveBatchPhyRel {
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

  private ExprNode serializeAggCall(AggregateCall aggCall) {
    checkArgument(aggCall.getAggregation().kind != SqlKind.AVG, "avg is not yet supported");
    List<RexNode> rexArgs = new ArrayList<>();
    for (int i : aggCall.getArgList()) {
      rexArgs.add(getCluster().getRexBuilder().makeInputRef(input, i));
    }
    RexNode rexAggCall =
        getCluster().getRexBuilder().makeCall(aggCall.getType(), aggCall.getAggregation(), rexArgs);
    return rexAggCall.accept(new RexToProtoSerializer());
  }

  private SimpleAggNode serializeSimpleAgg() {
    SimpleAggNode.Builder simpleAggNodeBuilder = SimpleAggNode.newBuilder();
    for (AggregateCall aggCall : aggCalls) {
      simpleAggNodeBuilder.addAggregations(serializeAggCall(aggCall));
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
