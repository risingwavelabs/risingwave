package com.risingwave.planner.rules.streaming;

import static com.risingwave.execution.context.ExecutionContext.contextOf;
import static com.risingwave.planner.planner.PlannerUtils.isSingleMode;
import static com.risingwave.planner.rel.physical.streaming.RisingWaveStreamingRel.STREAMING;

import com.risingwave.planner.rel.common.dist.RwDistributionTrait;
import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.physical.streaming.RwStreamExchange;
import com.risingwave.planner.rel.physical.streaming.RwStreamTableSource;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;

public class StreamingSourceDispatchRule extends RelRule<StreamingSourceDispatchRule.Config> {
  // This rule add distribution trait to RwStreamTableSource nodes. The trait will be later expanded
  // as exchange nodes in StreamExpandExchangeRule.
  protected StreamingSourceDispatchRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    var context = contextOf(call);
    var isSingleMode = isSingleMode(context);
    RwStreamTableSource streamTableSource = call.rel(0);

    // For now, we assume the streaming engine shares the same cluster with OLAP engine.
    return !isSingleMode && !streamTableSource.isDispatched();
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RwStreamTableSource streamTableSource = call.rel(0);
    streamTableSource.setDispatched();

    int[] distFields = {0};
    RwDistributionTrait distributionTrait = RwDistributions.hash(distFields);

    // We still need to convert the outTrait of sources into distributionTrait to avoid infinite
    // recursion.
    var sourceRequiredTraits =
        streamTableSource.getTraitSet().plus(STREAMING).plus(distributionTrait);
    var convertedSource = RelOptRule.convert(streamTableSource, sourceRequiredTraits);

    var exchange = RwStreamExchange.create(streamTableSource, distributionTrait);
    call.transformTo(exchange);
  }

  public interface Config extends RelRule.Config {
    StreamingSourceDispatchRule.Config DEFAULT =
        RelRule.Config.EMPTY
            .withDescription("Adding dispatchers at the output of streaming source")
            .withOperandSupplier(s -> s.operand(RwStreamTableSource.class).anyInputs())
            .as(StreamingSourceDispatchRule.Config.class);

    @Override
    default RelOptRule toRule() {
      return new StreamingSourceDispatchRule(this);
    }
  }
}
