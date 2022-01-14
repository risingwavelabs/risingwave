package com.risingwave.planner.rules.distributed.agg;

import static com.risingwave.planner.rel.physical.RisingWaveBatchPhyRel.BATCH_DISTRIBUTED;
import static com.risingwave.planner.rel.physical.RisingWaveBatchPhyRel.BATCH_PHYSICAL;
import static com.risingwave.planner.rules.distributed.agg.SplitUtils.getAggSplitters;
import static com.risingwave.planner.rules.distributed.agg.SplitUtils.getGlobalAggCalls;
import static com.risingwave.planner.rules.distributed.agg.SplitUtils.getLocalAggCalls;
import static java.util.Collections.emptyList;

import com.google.common.collect.ImmutableList;
import com.risingwave.planner.rel.common.dist.RwDistributionTrait;
import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.physical.RwAggregate;
import com.risingwave.planner.rel.physical.RwBatchProject;
import com.risingwave.planner.rules.aggspliter.AggSplitter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Rule converting a RwAggregate to 2-phase distributed version */
public class TwoPhaseAggRule extends ConverterRule {

  public static final TwoPhaseAggRule INSTANCE =
      ConverterRule.Config.INSTANCE
          .withInTrait(BATCH_PHYSICAL)
          .withOutTrait(BATCH_DISTRIBUTED)
          .withRuleFactory(TwoPhaseAggRule::new)
          .withOperandSupplier(t -> t.operand(RwAggregate.class).anyInputs())
          .withDescription("split agg 2-phase agg.")
          .as(ConverterRule.Config.class)
          .toRule(TwoPhaseAggRule.class);

  protected TwoPhaseAggRule(Config config) {
    super(config);
  }

  @Override
  public @Nullable RelNode convert(RelNode rel) {
    var agg = (RwAggregate) rel;
    var splitters = getAggSplitters(agg);

    var localAggCalls = getLocalAggCalls(agg, splitters);

    var requiredInputTraits = agg.getInput().getTraitSet().plus(BATCH_DISTRIBUTED);
    var newInput = RelOptRule.convert(agg.getInput(), requiredInputTraits);

    var localAgg =
        agg.copy(
            agg.getTraitSet().replace(BATCH_DISTRIBUTED),
            newInput,
            agg.getGroupSet(),
            agg.getGroupSets(),
            localAggCalls.stream().flatMap(List::stream).collect(Collectors.toList()));

    RwDistributionTrait globalDistribution;
    if (agg.getGroupCount() == 0) {
      globalDistribution = RwDistributions.SINGLETON;
    } else {
      globalDistribution = RwDistributions.hash(agg.getGroupKeys());
    }
    var globalInput =
        RelOptRule.convert(
            localAgg, localAgg.getTraitSet().plus(BATCH_DISTRIBUTED).plus(globalDistribution));

    var globalAggCalls = getGlobalAggCalls(agg, localAgg, splitters);

    // The global aggregation's group set should be normalized as it refers to the position
    // in local aggregation instead of the input to the original aggregation.
    ImmutableBitSet newGlobalAggGroupSet = ImmutableBitSet.range(localAgg.getGroupCount());
    ImmutableList<ImmutableBitSet> newGlobalAggGroupSets = ImmutableList.of(newGlobalAggGroupSet);

    var globalAgg =
        (RwAggregate)
            agg.copy(
                agg.getTraitSet().replace(BATCH_DISTRIBUTED).plus(globalDistribution),
                globalInput,
                newGlobalAggGroupSet,
                newGlobalAggGroupSets,
                globalAggCalls.stream().flatMap(List::stream).collect(Collectors.toList()));

    var allLastCalcAreTrivial = splitters.stream().allMatch(AggSplitter::isLastCalcTrivial);

    if (allLastCalcAreTrivial) {
      return globalAgg;
    } else {
      var rexBuilder = agg.getCluster().getRexBuilder();
      var expressions = new ArrayList<RexNode>(agg.getGroupCount() + splitters.size());
      agg.getGroupSet().toList().stream()
          .map(idx -> rexBuilder.makeInputRef(globalAgg, idx))
          .forEachOrdered(expressions::add);

      expressions.addAll(SplitUtils.getLastCalcs(agg, globalAgg, splitters));

      return new RwBatchProject(
          agg.getCluster(),
          globalAgg.getTraitSet().replace(BATCH_DISTRIBUTED).plus(globalDistribution),
          emptyList(),
          globalAgg,
          expressions,
          agg.getRowType());
    }
  }
}
