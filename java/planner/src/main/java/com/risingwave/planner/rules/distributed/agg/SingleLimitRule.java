package com.risingwave.planner.rules.distributed.agg;

import static com.risingwave.planner.rel.physical.RisingWaveBatchPhyRel.BATCH_DISTRIBUTED;
import static com.risingwave.planner.rel.physical.RisingWaveBatchPhyRel.BATCH_PHYSICAL;

import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.physical.RwBatchLimit;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Rule converting a RwBatchLimit to singleton exchange then limit */
public class SingleLimitRule extends ConverterRule {

  public static final SingleLimitRule INSTANCE =
      Config.INSTANCE
          .withInTrait(BATCH_PHYSICAL)
          .withOutTrait(BATCH_DISTRIBUTED)
          .withRuleFactory(SingleLimitRule::new)
          .withOperandSupplier(t -> t.operand(RwBatchLimit.class).anyInputs())
          .withDescription("convert limit to shuffle limit.")
          .as(Config.class)
          .toRule(SingleLimitRule.class);

  protected SingleLimitRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    RelNode rel = call.rel(0);
    RwBatchLimit limit = (RwBatchLimit) rel;
    RexNode fetch = limit.getFetch();
    return fetch == null;
  }

  @Override
  public @Nullable RelNode convert(RelNode rel) {
    RwBatchLimit limit = (RwBatchLimit) rel;
    RexNode offset = limit.getOffset();
    RexNode fetch = limit.getFetch();
    assert (fetch == null);
    var requiredInputTraits =
        limit.getInput().getTraitSet().plus(BATCH_DISTRIBUTED).plus(RwDistributions.SINGLETON);
    var newInput = RelOptRule.convert(limit.getInput(), requiredInputTraits);
    var newLimit =
        new RwBatchLimit(
            limit.getCluster(),
            limit.getTraitSet().plus(BATCH_DISTRIBUTED).plus(RwDistributions.SINGLETON),
            newInput,
            offset,
            fetch);
    return newLimit;
  }
}
