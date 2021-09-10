package com.risingwave.planner.rel.logical;

import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RwLogicalAggregate extends Aggregate implements RisingWaveLogicalRel {
  private RwLogicalAggregate(
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
  public Aggregate copy(
      RelTraitSet traitSet,
      RelNode input,
      ImmutableBitSet groupSet,
      @Nullable List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    return new RwLogicalAggregate(
        getCluster(), traitSet, getHints(), input, groupSet, groupSets, aggCalls);
  }

  public boolean isSimpleAgg() {
    return groupSet.isEmpty();
  }

  public static class RwAggregateConverterRule extends ConverterRule {
    public static final RwAggregateConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(Convention.NONE)
            .withOutTrait(LOGICAL)
            .withRuleFactory(RwAggregateConverterRule::new)
            .withOperandSupplier(t -> t.operand(LogicalAggregate.class).anyInputs())
            .withDescription("RisingWaveLogicalAggregateConverter")
            .as(Config.class)
            .toRule(RwAggregateConverterRule.class);

    protected RwAggregateConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      LogicalAggregate logicalAgg = (LogicalAggregate) rel;

      RelTraitSet newTraitSet = rel.getTraitSet().plus(LOGICAL);
      RelNode newInput = RelOptRule.convert(logicalAgg.getInput(), LOGICAL);
      return new RwLogicalAggregate(
          logicalAgg.getCluster(),
          newTraitSet,
          logicalAgg.getHints(),
          newInput,
          logicalAgg.getGroupSet(),
          logicalAgg.getGroupSets(),
          logicalAgg.getAggCallList());
    }
  }
}
