package com.risingwave.planner.rel.physical.batch;

import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;

import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.logical.RwLogicalGather;
import com.risingwave.proto.plan.PlanNode;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.convert.ConverterRule;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Physical version console operator.
 *
 * @see RwLogicalGather
 */
public class RwBatchGather extends SingleRel implements RisingWaveBatchPhyRel {
  private RwBatchGather(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
    super(cluster, traits, input);
    checkConvention();
  }

  @Override
  public PlanNode serialize() {
    throw new UnsupportedOperationException("RwBatchGather#serialize is not supported");
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new RwBatchGather(getCluster(), traitSet, sole(inputs));
  }

  public static class RwBatchGatherConverterRule extends ConverterRule {
    public static final RwBatchGatherConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(LOGICAL)
            .withOutTrait(BATCH_PHYSICAL)
            .withDescription("Logical console to batch console")
            .withOperandSupplier(t -> t.operand(RwLogicalGather.class).anyInputs())
            .as(Config.class)
            .withRuleFactory(RwBatchGatherConverterRule::new)
            .toRule(RwBatchGatherConverterRule.class);

    private RwBatchGatherConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      var logical = (RwLogicalGather) rel;
      var inputRequiredTraits =
          logical.getInput().getTraitSet().plus(BATCH_PHYSICAL).plus(RwDistributions.SINGLETON);
      var newInput = RelOptRule.convert(logical.getInput(), inputRequiredTraits);

      var newTraits = logical.getTraitSet().plus(BATCH_PHYSICAL);

      return new RwBatchGather(logical.getCluster(), newTraits, newInput);
    }
  }
}
