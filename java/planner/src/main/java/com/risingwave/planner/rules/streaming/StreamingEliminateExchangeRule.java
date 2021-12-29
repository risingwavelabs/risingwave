package com.risingwave.planner.rules.streaming;

import com.risingwave.planner.rel.streaming.RwStreamExchange;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;

/**
 * Rule for eliminating the upstream exchange when there are two consecutive exchanges in streaming
 * convention
 */
public class StreamingEliminateExchangeRule extends RelRule<StreamingEliminateExchangeRule.Config> {

  public StreamingEliminateExchangeRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RwStreamExchange topExchangeOperator = call.rel(0);
    RwStreamExchange downExchangeOperator = call.rel(1);

    RwStreamExchange newTopExchangeOperator =
        new RwStreamExchange(
            topExchangeOperator.getCluster(),
            topExchangeOperator.getTraitSet(),
            downExchangeOperator.getInput(),
            topExchangeOperator.getDistribution());

    // TODO: remove upstreamSet, this will be set and send to compute node in meta service.
    downExchangeOperator.getUpstreamSet().forEach(newTopExchangeOperator::addUpStream);

    call.transformTo(newTopExchangeOperator);
  }

  /** Default config */
  public interface Config extends RelRule.Config {
    StreamingEliminateExchangeRule.Config DEFAULT =
        RelRule.Config.EMPTY
            .withDescription("Eliminate the upstream Exchange of two consecutive exchanges")
            .withOperandSupplier(
                s ->
                    s.operand(RwStreamExchange.class)
                        .oneInput(b -> b.operand(RwStreamExchange.class).anyInputs()))
            .as(StreamingEliminateExchangeRule.Config.class);

    @Override
    default RelOptRule toRule() {
      return new StreamingEliminateExchangeRule(this);
    }
  }
}
