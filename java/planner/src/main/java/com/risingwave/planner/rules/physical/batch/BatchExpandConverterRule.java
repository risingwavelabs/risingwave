package com.risingwave.planner.rules.physical.batch;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.risingwave.planner.rel.physical.batch.RisingWaveBatchPhyRel.BATCH_PHYSICAL;

import com.risingwave.planner.rel.common.dist.RwDistributionTrait;
import com.risingwave.planner.rel.common.dist.RwDistributionTraitDef;
import com.risingwave.planner.rel.physical.batch.RwBatchExchange;
import com.risingwave.planner.rel.physical.batch.RwBatchSort;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;

public class BatchExpandConverterRule extends RelRule<BatchExpandConverterRule.Config> {
  private BatchExpandConverterRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    var toTraits = call.rel(0).getTraitSet();
    var fromTraits = call.rel(1).getTraitSet();

    return toTraits.contains(BATCH_PHYSICAL)
        && fromTraits.contains(BATCH_PHYSICAL)
        && !fromTraits.satisfies(toTraits);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    var toTraits = call.rel(0).getTraitSet();

    var ret = call.rel(1);
    if (toTraits.getTrait(RwDistributionTraitDef.getInstance()) != null) {
      ret = satisfyDistribution(ret, toTraits.getTrait(RwDistributionTraitDef.getInstance()));
    }

    if (toTraits.getCollation() != null) {
      ret = satisfyCollation(ret, toTraits.getCollation());
    }

    verify(ret.getTraitSet().satisfies(toTraits), "Traits not satisfied!");
    call.transformTo(ret);
  }

  private static RelNode satisfyDistribution(RelNode rel, RwDistributionTrait toDist) {
    checkArgument(rel.getTraitSet().contains(BATCH_PHYSICAL), "Can't convert logical trait!");
    if (toDist.getType() == RelDistribution.Type.ANY) {
      return rel;
    }

    if (toDist.equals(rel.getTraitSet().getTrait(RwDistributionTraitDef.getInstance()))) {
      return rel;
    }

    return RwBatchExchange.create(rel, toDist);
  }

  private static RelNode satisfyCollation(RelNode rel, RelCollation toCollation) {
    var fromCollation = rel.getTraitSet().getCollation();
    if (fromCollation != null && fromCollation.satisfies(toCollation)) {
      return rel;
    }

    var newTraits = rel.getTraitSet().plus(toCollation).plus(BATCH_PHYSICAL);
    return new RwBatchSort(
        rel.getCluster(), newTraits, rel, RelCollationTraitDef.INSTANCE.canonize(toCollation));
  }

  public interface Config extends RelRule.Config {
    Config DEFAULT =
        RelRule.Config.EMPTY
            .withDescription("Risingwave expand converter rule")
            .withOperandSupplier(
                s ->
                    s.operand(AbstractConverter.class)
                        .oneInput(b -> b.operand(RelNode.class).anyInputs()))
            .as(Config.class);

    @Override
    default RelOptRule toRule() {
      return new BatchExpandConverterRule(this);
    }
  }
}
