package com.risingwave.planner.rules.streaming.aggregate;

import static com.risingwave.execution.context.ExecutionContext.contextOf;
import static com.risingwave.planner.planner.PlannerUtils.isDistributedMode;
import static com.risingwave.planner.rel.streaming.RisingWaveStreamingRel.STREAMING;
import static com.risingwave.planner.rules.aggspliter.LogicalAggSplitUtils.getAggSplitters;
import static com.risingwave.planner.rules.aggspliter.LogicalAggSplitUtils.getGlobalAggCalls;
import static com.risingwave.planner.rules.aggspliter.LogicalAggSplitUtils.getLocalAggCalls;

import com.google.common.collect.ImmutableList;
import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.logical.RwLogicalAggregate;
import com.risingwave.planner.rel.streaming.RwStreamAgg;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.util.ImmutableBitSet;

/** Rule for converting logical aggregation to two phase stream aggregation */
public class StreamingTwoPhaseAggRule extends RelRule<StreamingTwoPhaseAggRule.Config> {

  private StreamingTwoPhaseAggRule(Config config) {
    super(config);
  }

  // In the future, we expect all the rules to generate all the
  // different plans and compare the plan cost by volcano planner
  // For now, we restrict that the shuffle exchange will only work when
  // the aggregation group count must be <= 1. And of course, the cluster
  // must be in distributed mode.
  @Override
  public boolean matches(RelOptRuleCall call) {
    RwLogicalAggregate logicalAgg = call.rel(0);
    var groupCount = logicalAgg.getGroupCount();
    boolean distributedMode = isDistributedMode(contextOf(call));
    return groupCount <= 1 && distributedMode;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RwLogicalAggregate logicalAgg = call.rel(0);
    assert isDistributedMode(contextOf(call));
    toTwoPhasePlan(logicalAgg, call);
  }

  private void toTwoPhasePlan(RwLogicalAggregate logicalAgg, RelOptRuleCall call) {
    var splitters = getAggSplitters(logicalAgg);

    var localAggTraits = logicalAgg.getTraitSet().plus(STREAMING);
    var localAggInputRequiredTraits = logicalAgg.getInput().getTraitSet().plus(STREAMING);
    var localAggInput = RelOptRule.convert(logicalAgg.getInput(), localAggInputRequiredTraits);
    var localAggCalls =
        getLocalAggCalls(logicalAgg, splitters).stream()
            .flatMap(List::stream)
            .collect(Collectors.toList());

    var localStreamAgg =
        new RwStreamAgg(
            logicalAgg.getCluster(),
            localAggTraits,
            logicalAgg.getHints(),
            localAggInput,
            logicalAgg.getGroupSet(),
            logicalAgg.getGroupSets(),
            localAggCalls);

    // For simple aggregation we use Singleton Distribution to gather all the results into a single
    // place.
    var globalDistTrait = RwDistributions.SINGLETON;
    if (!logicalAgg.isSimpleAgg()) {
      // For non-simple aggregation, we use Hash Distribution.
      globalDistTrait = RwDistributions.hash(logicalAgg.getGroupSet().toArray());
    }

    var globalAggInputRequiredTraits = localStreamAgg.getTraitSet().plus(globalDistTrait);

    var globalAggInput = RelOptRule.convert(localStreamAgg, globalAggInputRequiredTraits);

    var globalAggCalls =
        getGlobalAggCalls(logicalAgg, globalAggInput, splitters).stream()
            .flatMap(List::stream)
            .collect(Collectors.toList());

    // The global aggregation's group set should be normalized as it refers to the position
    // in local aggregation instead of the input to the original aggregation.
    ImmutableBitSet newGlobalAggGroupSet = ImmutableBitSet.range(localStreamAgg.getGroupCount());
    ImmutableList<ImmutableBitSet> newGlobalAggGroupSets = ImmutableList.of(newGlobalAggGroupSet);

    var newGlobalStreamAgg =
        new RwStreamAgg(
            globalAggInput.getCluster(),
            localAggTraits,
            localStreamAgg.getHints(),
            globalAggInput,
            newGlobalAggGroupSet,
            newGlobalAggGroupSets,
            globalAggCalls);

    call.transformTo(newGlobalStreamAgg);
  }

  /** Default config */
  public interface Config extends RelRule.Config {
    StreamingTwoPhaseAggRule.Config DEFAULT =
        RelRule.Config.EMPTY
            .withDescription("Converting logical agg to two phase aggregation")
            .withOperandSupplier(s -> s.operand(RwLogicalAggregate.class).anyInputs())
            .as(StreamingTwoPhaseAggRule.Config.class);

    @Override
    default RelOptRule toRule() {
      return new StreamingTwoPhaseAggRule(this);
    }
  }
}
