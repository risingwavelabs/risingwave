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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Logical Aggregate */
public class RwLogicalAggregate extends Aggregate implements RisingWaveLogicalRel {
  public RwLogicalAggregate(
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

  /** An Aggregate is simple if there is no group keys */
  public boolean isSimpleAgg() {
    return groupSet.isEmpty();
  }

  /** An aggregate can be executed use two-phase iff all aggregation calls are stateless. * */
  public boolean streamingCanTwoPhase() {
    return getAggCallList().stream()
        .allMatch(
            (agg) -> {
              var kind = agg.getAggregation().getKind();
              return kind == SqlKind.SUM || kind == SqlKind.COUNT;
            });
  }

  /** Rule to convert a Stream LogicalAggregate to RwLogicalAggregate */
  public static class RwBatchAggregateConverterRule extends ConverterRule {
    public static final RwBatchAggregateConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(Convention.NONE)
            .withOutTrait(LOGICAL)
            .withRuleFactory(RwBatchAggregateConverterRule::new)
            .withOperandSupplier(t -> t.operand(LogicalAggregate.class).anyInputs())
            .withDescription("RisingWaveBatchLogicalAggregateConverter")
            .as(Config.class)
            .toRule(RwBatchAggregateConverterRule.class);

    protected RwBatchAggregateConverterRule(Config config) {
      super(config);
    }

    /** Convert Batch LogicalAggregate to RwLogicalAggregate */
    @Override
    public @Nullable RelNode convert(RelNode rel) {
      LogicalAggregate logicalAgg = (LogicalAggregate) rel;

      RelNode newInput = RelOptRule.convert(logicalAgg.getInput(), LOGICAL);
      RelTraitSet newTraitSet = rel.getTraitSet().plus(LOGICAL);
      // TODO: We should also normalize batch plan.
      // return NormalizeAggUtils.convert(logicalAgg, false, newInput, newTraitSet);
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

  /** Rule to convert a Stream LogicalAggregate to RwLogicalAggregate */
  public static class RwStreamAggregateConverterRule extends ConverterRule {
    public static final RwStreamAggregateConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(Convention.NONE)
            .withOutTrait(LOGICAL)
            .withRuleFactory(RwStreamAggregateConverterRule::new)
            .withOperandSupplier(t -> t.operand(LogicalAggregate.class).anyInputs())
            .withDescription("RisingWaveBatchLogicalAggregateConverter")
            .as(Config.class)
            .toRule(RwStreamAggregateConverterRule.class);

    protected RwStreamAggregateConverterRule(Config config) {
      super(config);
    }

    /** Convert Stream LogicalAggregate to RwLogicalAggregate */
    @Override
    public @Nullable RelNode convert(RelNode rel) {
      LogicalAggregate logicalAgg = (LogicalAggregate) rel;

      RelNode newInput = RelOptRule.convert(logicalAgg.getInput(), LOGICAL);
      RelTraitSet newTraitSet = rel.getTraitSet().plus(LOGICAL);
      return NormalizeAggUtils.convert(logicalAgg, true, newInput, newTraitSet);
    }
  }
}
