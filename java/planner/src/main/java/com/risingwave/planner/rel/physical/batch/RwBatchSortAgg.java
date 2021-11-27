package com.risingwave.planner.rel.physical.batch;

import static com.risingwave.common.config.BatchPlannerConfigurations.ENABLE_SORT_AGG;
import static com.risingwave.execution.context.ExecutionContext.contextOf;
import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.risingwave.planner.rel.common.dist.RwDistributionTraitDef;
import com.risingwave.planner.rel.logical.RwLogicalAggregate;
import com.risingwave.planner.rel.physical.RwAggregate;
import com.risingwave.planner.rel.serialization.RexToProtoSerializer;
import com.risingwave.proto.plan.PlanNode;
import com.risingwave.proto.plan.SortAggNode;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Sort agg assumes that its input are sorted according to it group key. */
public class RwBatchSortAgg extends RwAggregate implements RisingWaveBatchPhyRel, PhysicalNode {
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

  @Override
  public Aggregate copy(
      RelTraitSet traitSet,
      RelNode input,
      ImmutableBitSet groupSet,
      @Nullable List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    return new RwBatchSortAgg(getCluster(), traitSet, hints, input, groupSet, groupSets, aggCalls);
  }

  /** SortAgg converter rule between logical and physical. */
  public static class BatchSortAggConverterRule extends ConverterRule {
    public static final RwBatchSortAgg.BatchSortAggConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(LOGICAL)
            .withOutTrait(BATCH_PHYSICAL)
            .withRuleFactory(RwBatchSortAgg.BatchSortAggConverterRule::new)
            .withOperandSupplier(t -> t.operand(RwLogicalAggregate.class).anyInputs())
            .withDescription("Converting logical agg to batch sort agg.")
            .as(Config.class)
            .toRule(RwBatchSortAgg.BatchSortAggConverterRule.class);

    protected BatchSortAggConverterRule(Config config) {
      super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      var agg = (RwLogicalAggregate) call.rel(0);
      // we treat simple agg(without groups) as a special sortAgg.
      if (agg.isSimpleAgg()) {
        return true;
      }
      return contextOf(call).getConf().get(ENABLE_SORT_AGG);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      var agg = (RwLogicalAggregate) rel;
      var requiredInputTraits = agg.getInput().getTraitSet().replace(BATCH_PHYSICAL);
      var aggTrait = agg.getTraitSet().plus(BATCH_PHYSICAL);
      if (!agg.isSimpleAgg()) {
        var collation = RelCollations.of(ImmutableIntList.copyOf(agg.getGroupSet().asList()));
        requiredInputTraits = requiredInputTraits.plus(collation);
        aggTrait = aggTrait.plus(collation);
      }
      RelNode newInput = RelOptRule.convert(agg.getInput(), requiredInputTraits);

      return new RwBatchSortAgg(
          rel.getCluster(),
          aggTrait,
          agg.getHints(),
          newInput,
          agg.getGroupSet(),
          agg.getGroupSets(),
          agg.getAggCallList());
    }
  }
}
