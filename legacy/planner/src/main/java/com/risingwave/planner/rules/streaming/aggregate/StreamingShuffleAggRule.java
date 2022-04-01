package com.risingwave.planner.rules.streaming.aggregate;

import static com.risingwave.execution.context.ExecutionContext.contextOf;
import static com.risingwave.planner.planner.PlannerUtils.isDistributedMode;
import static com.risingwave.planner.rel.streaming.RisingWaveStreamingRel.STREAMING;

import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.logical.RwLogicalAggregate;
import com.risingwave.planner.rel.streaming.RwStreamAgg;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;

/** Rule for converting logical aggregation to shuffle aggregation */
public class StreamingShuffleAggRule extends RelRule<StreamingShuffleAggRule.Config> {

  //  /** Threshold of number of group keys to use shuffle aggregation */
  //  private static final int MIN_GROUP_COUNT = 2;

  public StreamingShuffleAggRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    RwLogicalAggregate logicalAgg = call.rel(0);
    var groupCount = logicalAgg.getGroupCount();
    return isDistributedMode(contextOf(call)) && groupCount > 0;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RwLogicalAggregate logicalAgg = call.rel(0);
    var groupCount = logicalAgg.getGroupCount();
    //    assert isDistributedMode(contextOf(call)) && groupCount >= MIN_GROUP_COUNT;
    var distTrait = RwDistributions.hash(logicalAgg.getGroupSet().toArray());
    var requiredInputTraitSet = logicalAgg.getInput().getTraitSet().plus(STREAMING).plus(distTrait);
    var aggTraits = logicalAgg.getTraitSet().plus(STREAMING);
    var newInput = RelOptRule.convert(logicalAgg.getInput(), requiredInputTraitSet);

    var streamAgg =
        new RwStreamAgg(
            logicalAgg.getCluster(),
            aggTraits,
            logicalAgg.getHints(),
            newInput,
            logicalAgg.getGroupSet(),
            logicalAgg.getGroupSets(),
            logicalAgg.getAggCallList());

    call.transformTo(streamAgg);
  }

  /** Default config */
  public interface Config extends RelRule.Config {
    StreamingShuffleAggRule.Config DEFAULT =
        RelRule.Config.EMPTY
            .withDescription("Converting logical agg to shuffle aggregation")
            .withOperandSupplier(s -> s.operand(RwLogicalAggregate.class).anyInputs())
            .as(StreamingShuffleAggRule.Config.class);

    @Override
    default RelOptRule toRule() {
      return new StreamingShuffleAggRule(this);
    }
  }
}
