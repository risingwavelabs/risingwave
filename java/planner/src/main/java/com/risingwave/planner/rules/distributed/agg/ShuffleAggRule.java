package com.risingwave.planner.rules.distributed.agg;

import static com.risingwave.planner.rel.physical.RisingWaveBatchPhyRel.BATCH_DISTRIBUTED;
import static com.risingwave.planner.rel.physical.RisingWaveBatchPhyRel.BATCH_PHYSICAL;

import com.google.common.collect.ImmutableList;
import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.physical.RwBatchHashAgg;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Rule converting a HashAgg to distributed version shard by group key */
public class ShuffleAggRule extends ConverterRule {
  public static final ShuffleAggRule INSTANCE =
      Config.INSTANCE
          .withInTrait(BATCH_PHYSICAL)
          .withOutTrait(BATCH_DISTRIBUTED)
          .withRuleFactory(ShuffleAggRule::new)
          .withOperandSupplier(t -> t.operand(RwBatchHashAgg.class).anyInputs())
          .withDescription("convert to shuffle agg.")
          .as(Config.class)
          .toRule(ShuffleAggRule.class);

  protected ShuffleAggRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    var agg = (RwBatchHashAgg) call.rel(0);
    return agg.getGroupCount() != 0;
  }

  @Override
  public @Nullable RelNode convert(RelNode rel) {
    var agg = (RwBatchHashAgg) rel;
    var input = agg.getInput();
    var newInput =
        RelOptRule.convert(
            input,
            input
                .getTraitSet()
                .plus(BATCH_DISTRIBUTED)
                .plus(RwDistributions.hash(agg.getGroupSet().toArray())));
    return agg.copy(
        agg.getTraitSet().plus(BATCH_DISTRIBUTED).plus(RwDistributions.hash(agg.getGroupKeys())),
        ImmutableList.of(newInput));
  }
}
