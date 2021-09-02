package com.risingwave.planner.rules.logical;

import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;

import com.risingwave.planner.rel.logical.RwLogicalGather;
import java.util.Collections;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;

public class GatherConversionRule extends RelRule<GatherConversionRule.Config> {
  protected GatherConversionRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RwLogicalGather gather = call.rel(0);

    var input = gather.getInput();
    var newInput = RelOptRule.convert(input, input.getTraitSet().plus(LOGICAL));

    var newGather = gather.copy(gather.getTraitSet(), Collections.singletonList(newInput));
    call.transformTo(newGather);
  }

  public interface Config extends RelRule.Config {
    Config DEFAULT =
        EMPTY
            .withDescription("Converting logical gather")
            .withOperandSupplier(t -> t.operand(RwLogicalGather.class).anyInputs())
            .as(Config.class);

    @Override
    default RelOptRule toRule() {
      return new GatherConversionRule(this);
    }
  }
}
