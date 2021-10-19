package com.risingwave.planner.rules.streaming;

import static com.risingwave.execution.context.ExecutionContext.contextOf;
import static com.risingwave.planner.planner.PlannerUtils.isSingleMode;

import com.risingwave.planner.rel.common.dist.RwDistributionTrait;
import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.physical.streaming.RwStreamExchange;
import com.risingwave.planner.rel.physical.streaming.RwStreamTableSource;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;

/** Rule for adding RwStreamExchange immediately after RwStreamTableSource */
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

    var exchange = RwStreamExchange.create(streamTableSource, distributionTrait);
    call.transformTo(exchange);
  }

  /** Default config */
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
