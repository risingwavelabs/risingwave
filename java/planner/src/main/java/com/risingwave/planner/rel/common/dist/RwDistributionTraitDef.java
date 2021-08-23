package com.risingwave.planner.rel.common.dist;

import static com.google.common.base.Preconditions.checkArgument;
import static com.risingwave.planner.rel.physical.batch.RisingWaveBatchPhyRel.BATCH_PHYSICAL;

import com.risingwave.planner.rel.physical.batch.RwBatchExchange;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RwDistributionTraitDef extends RelTraitDef<RwDistributionTrait> {
  private static final RwDistributionTraitDef SINGLETON = new RwDistributionTraitDef();

  private RwDistributionTraitDef() {}

  public static RwDistributionTraitDef getInstance() {
    return SINGLETON;
  }

  @Override
  public Class<RwDistributionTrait> getTraitClass() {
    return RwDistributionTrait.class;
  }

  @Override
  public String getSimpleName() {
    return "RisingWaveDistribution";
  }

  @Override
  public @Nullable RelNode convert(
      RelOptPlanner planner,
      RelNode rel,
      RwDistributionTrait toDist,
      boolean allowInfiniteCostConverters) {
    checkArgument(rel.getTraitSet().contains(BATCH_PHYSICAL), "Can't convert logical trait!");
    if (toDist.getType() == RelDistribution.Type.ANY) {
      return rel;
    }

    if (toDist.equals(rel.getTraitSet().getTrait(RwDistributionTraitDef.getInstance()))) {
      return rel;
    }

    final RwBatchExchange exchange = RwBatchExchange.create(rel, toDist);
    RelNode newRel = planner.register(exchange, rel);
    final RelTraitSet newTraitSet = rel.getTraitSet().plus(toDist);
    if (!newRel.getTraitSet().equals(newTraitSet)) {
      newRel = planner.changeTraits(newRel, newTraitSet);
    }
    return newRel;
  }

  @Override
  public boolean canConvert(
      RelOptPlanner planner, RwDistributionTrait fromTrait, RwDistributionTrait toTrait) {
    return true;
  }

  @Override
  public RwDistributionTrait getDefault() {
    return RwDistributions.ANY;
  }
}
