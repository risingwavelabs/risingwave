package com.risingwave.planner.rel.physical.batch;

import static com.google.common.base.Verify.verify;

import com.risingwave.planner.rel.common.dist.RwDistributionTrait;
import com.risingwave.proto.plan.PlanNode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Exchange;

public class RwBatchExchange extends Exchange implements RisingWaveBatchPhyRel {
  private final int uniqueId;

  private RwBatchExchange(
      RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelDistribution distribution) {
    super(cluster, traitSet, input, distribution);
    checkConvention();
    verify(
        traitSet.contains(distribution), "Trait set: %s, distribution: %s", traitSet, distribution);

    this.uniqueId = super.getId();
  }

  @Override
  public Exchange copy(RelTraitSet traitSet, RelNode newInput, RelDistribution newDistribution) {
    return new RwBatchExchange(getCluster(), traitSet, newInput, newDistribution);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    var writer = super.explainTerms(pw);
    var collation = getTraitSet().getCollation();
    if (collation != null) {
      writer.item("collation", collation);
    }

    return writer;
  }

  @Override
  public PlanNode serialize() {
    // Exchange is a pipeline breaker, we do not serialize its children.
    return PlanNode.newBuilder().setNodeType(PlanNode.PlanNodeType.EXCHANGE).build();
  }

  public static RwBatchExchange create(RelNode input, RwDistributionTrait distribution) {
    RelOptCluster cluster = input.getCluster();
    RelTraitSet traitSet = input.getTraitSet().plus(BATCH_PHYSICAL).plus(distribution);
    return new RwBatchExchange(cluster, traitSet, input, distribution);
  }

  // Every call will return the same id.
  public int getUniqueId() {
    return uniqueId;
  }
}
