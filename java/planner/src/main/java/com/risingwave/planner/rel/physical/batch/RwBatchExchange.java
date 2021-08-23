package com.risingwave.planner.rel.physical.batch;

import static com.google.common.base.Verify.verify;

import com.risingwave.planner.rel.common.dist.RwDistributionTrait;
import com.risingwave.proto.plan.PlanNode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;

public class RwBatchExchange extends Exchange implements RisingWaveBatchPhyRel {
  private RwBatchExchange(
      RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelDistribution distribution) {
    super(cluster, traitSet, input, distribution);
    checkConvention();
    verify(
        traitSet.contains(distribution), "Trait set: %s, distribution: %s", traitSet, distribution);
  }

  @Override
  public Exchange copy(RelTraitSet traitSet, RelNode newInput, RelDistribution newDistribution) {
    return new RwBatchExchange(getCluster(), traitSet, newInput, newDistribution);
  }

  @Override
  public PlanNode serialize() {
    throw new UnsupportedOperationException();
  }

  public static RwBatchExchange create(RelNode input, RwDistributionTrait distribution) {
    RelOptCluster cluster = input.getCluster();
    RelTraitSet traitSet = input.getTraitSet().replace(BATCH_PHYSICAL).replace(distribution);
    return new RwBatchExchange(cluster, traitSet, input, distribution);
  }
}
