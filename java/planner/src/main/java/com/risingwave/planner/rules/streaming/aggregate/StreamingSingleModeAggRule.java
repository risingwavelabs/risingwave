package com.risingwave.planner.rules.streaming.aggregate;

import static com.risingwave.execution.context.ExecutionContext.contextOf;
import static com.risingwave.planner.planner.PlannerUtils.isSingleMode;
import static com.risingwave.planner.rel.streaming.RisingWaveStreamingRel.STREAMING;

import com.risingwave.planner.rel.logical.RwLogicalAggregate;
import com.risingwave.planner.rel.streaming.RwStreamAgg;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;

/**
 * The rule for converting logical aggregation into single mode aggregation. No Exchange is needed
 * and added
 */
public class StreamingSingleModeAggRule extends RelRule<StreamingSingleModeAggRule.Config> {

  public StreamingSingleModeAggRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return isSingleMode(contextOf(call));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RwLogicalAggregate logicalAgg = call.rel(0);
    var requiredInputTraitSet = logicalAgg.getInput().getTraitSet().plus(STREAMING);
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
    StreamingSingleModeAggRule.Config DEFAULT =
        RelRule.Config.EMPTY
            .withDescription("Converting logical agg to single mode aggregation")
            .withOperandSupplier(s -> s.operand(RwLogicalAggregate.class).anyInputs())
            .as(StreamingSingleModeAggRule.Config.class);

    @Override
    default RelOptRule toRule() {
      return new StreamingSingleModeAggRule(this);
    }
  }
}
