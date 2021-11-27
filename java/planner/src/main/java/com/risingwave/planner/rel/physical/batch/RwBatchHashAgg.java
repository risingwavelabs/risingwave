package com.risingwave.planner.rel.physical.batch;

import static com.risingwave.common.config.BatchPlannerConfigurations.ENABLE_HASH_AGG;
import static com.risingwave.execution.context.ExecutionContext.contextOf;
import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.risingwave.planner.rel.common.dist.RwDistributionTraitDef;
import com.risingwave.planner.rel.logical.RwLogicalAggregate;
import com.risingwave.planner.rel.physical.RwAggregate;
import com.risingwave.proto.plan.HashAggNode;
import com.risingwave.proto.plan.PlanNode;
import com.risingwave.proto.plan.SimpleAggNode;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Plan for adhoc HashAgg executor. */
public class RwBatchHashAgg extends RwAggregate implements RisingWaveBatchPhyRel, PhysicalNode {
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

  @Override
  public @Nullable Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
    return null;
  }

  @Override
  public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(
      final RelTraitSet childTraits, final int childId) {
    if (childTraits.getConvention() != traitSet.getConvention()) {
      return null;
    }
    if (childTraits.getConvention() != BATCH_DISTRIBUTED) {
      return null;
    }

    var newTraits = traitSet;
    var dist = childTraits.getTrait(RwDistributionTraitDef.getInstance());
    if (dist != null) {
      newTraits = newTraits.plus(aggDistributionDerive(this, dist));
    }
    return Pair.of(newTraits, ImmutableList.of(input.getTraitSet()));
  }

  /** HashAgg converter rule between logical and physical. */
  public static class BatchHashAggConverterRule extends ConverterRule {
    public static final RwBatchHashAgg.BatchHashAggConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(LOGICAL)
            .withOutTrait(BATCH_PHYSICAL)
            .withRuleFactory(RwBatchHashAgg.BatchHashAggConverterRule::new)
            .withOperandSupplier(t -> t.operand(RwLogicalAggregate.class).anyInputs())
            .withDescription("Converting logical agg to batch hash agg.")
            .as(Config.class)
            .toRule(RwBatchHashAgg.BatchHashAggConverterRule.class);

    protected BatchHashAggConverterRule(Config config) {
      super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      var agg = (RwLogicalAggregate) call.rel(0);
      // we treat simple agg(without groups) as a special sortAgg.
      if (agg.isSimpleAgg()) {
        return false;
      }
      return contextOf(call).getConf().get(ENABLE_HASH_AGG);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      var agg = (RwLogicalAggregate) rel;
      RelTraitSet requiredInputTraits = agg.getInput().getTraitSet().replace(BATCH_PHYSICAL);
      RelNode newInput = RelOptRule.convert(agg.getInput(), requiredInputTraits);
      return new RwBatchHashAgg(
          rel.getCluster(),
          agg.getTraitSet().plus(BATCH_PHYSICAL),
          agg.getHints(),
          newInput,
          agg.getGroupSet(),
          agg.getGroupSets(),
          agg.getAggCallList());
    }
  }
}
