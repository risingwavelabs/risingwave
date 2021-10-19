package com.risingwave.planner.rules.streaming;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.risingwave.planner.rel.physical.streaming.RisingWaveStreamingRel.STREAMING;

import com.risingwave.planner.rel.common.dist.RwDistributionTrait;
import com.risingwave.planner.rel.common.dist.RwDistributionTraitDef;
import com.risingwave.planner.rel.physical.streaming.RwStreamExchange;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;

/** Rule for replacing AbstractConvertor with RwStreamExchange */
public class StreamingExpandConverterRule extends RelRule<StreamingExpandConverterRule.Config> {

  private StreamingExpandConverterRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    RelNode toRelNode = call.rel(0);
    RelNode fromRelNode = call.rel(1);
    RelTraitSet toTraits = toRelNode.getTraitSet();
    RelTraitSet fromTraits = fromRelNode.getTraitSet();

    return toTraits.contains(STREAMING)
        && fromTraits.contains(STREAMING)
        && !fromTraits.satisfies(toTraits);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    var toTraits = call.rel(0).getTraitSet();

    var ret = call.rel(1);
    if (toTraits.getTrait(RwDistributionTraitDef.getInstance()) != null) {
      ret = satisfyDistribution(ret, toTraits.getTrait(RwDistributionTraitDef.getInstance()));
    }
    // TODO: Collation?

    verify(ret.getTraitSet().satisfies(toTraits), "Traits not satisfied!");
    call.transformTo(ret);
  }

  private static RelNode satisfyDistribution(RelNode rel, RwDistributionTrait toDist) {
    checkArgument(rel.getTraitSet().contains(STREAMING), "Can't convert logical trait!");
    if (toDist.getType() == RelDistribution.Type.ANY) {
      return rel;
    }

    if (toDist.equals(rel.getTraitSet().getTrait(RwDistributionTraitDef.getInstance()))) {
      return rel;
    }

    return RwStreamExchange.create(rel, toDist);
  }

  /** Default config */
  public interface Config extends RelRule.Config {
    StreamingExpandConverterRule.Config DEFAULT =
        RelRule.Config.EMPTY
            .withDescription("Risingwave Streaming expand converter rule")
            .withOperandSupplier(
                s ->
                    s.operand(AbstractConverter.class)
                        .oneInput(b -> b.operand(RelNode.class).anyInputs()))
            .as(StreamingExpandConverterRule.Config.class);

    @Override
    default RelOptRule toRule() {
      return new StreamingExpandConverterRule(this);
    }
  }
}
