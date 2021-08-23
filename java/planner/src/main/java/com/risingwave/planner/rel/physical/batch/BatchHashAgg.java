package com.risingwave.planner.rel.physical.batch;

import static com.google.common.base.Preconditions.checkArgument;
import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;

import com.google.protobuf.Any;
import com.risingwave.planner.rel.logical.RwAggregate;
import com.risingwave.planner.rel.serialization.RexToProtoSerializer;
import com.risingwave.proto.expr.ExprNode;
import com.risingwave.proto.plan.HashAggNode;
import com.risingwave.proto.plan.PlanNode;
import com.risingwave.proto.plan.SimpleAggNode;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

public class BatchHashAgg extends Aggregate implements RisingWaveBatchPhyRel {
  protected BatchHashAgg(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelNode input,
      ImmutableBitSet groupSet,
      @Nullable List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    super(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls);
    checkArgument(traitSet.contains(RisingWaveBatchPhyRel.BATCH_PHYSICAL));
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

  private HashAggNode serializeHashAgg() {
    HashAggNode.Builder hashAggNodeBuilder = HashAggNode.newBuilder();
    for (int i = groupSet.nextSetBit(0); i >= 0; i = groupSet.nextSetBit(i + 1)) {
      hashAggNodeBuilder.addGroupByKeys(
          getCluster().getRexBuilder().makeInputRef(input, i).accept(new RexToProtoSerializer()));
    }
    for (AggregateCall aggCall : aggCalls) {
      hashAggNodeBuilder.addAggregations(serializeAggCall(aggCall));
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
    return new BatchHashAgg(
        getCluster(), traitSet, getHints(), input, groupSet, getGroupSets(), aggCalls);
  }

  public static class BatchHashAggConverterRule extends ConverterRule {
    public static final BatchHashAggConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(LOGICAL)
            .withOutTrait(BATCH_PHYSICAL)
            .withDescription("Logical aggregate to hash agg")
            .withOperandSupplier(t -> t.operand(RwAggregate.class).anyInputs())
            .as(Config.class)
            .withRuleFactory(BatchHashAggConverterRule::new)
            .toRule(BatchHashAggConverterRule.class);

    protected BatchHashAggConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      RwAggregate rwAggregate = (RwAggregate) rel;
      RelTraitSet newTraitSet = rwAggregate.getTraitSet().replace(BATCH_PHYSICAL);
      RelNode newInput = RelOptRule.convert(rwAggregate.getInput(), BATCH_PHYSICAL);

      return new BatchHashAgg(
          rwAggregate.getCluster(),
          newTraitSet,
          rwAggregate.getHints(),
          newInput,
          rwAggregate.getGroupSet(),
          rwAggregate.getGroupSets(),
          rwAggregate.getAggCallList());
    }
  }
}
