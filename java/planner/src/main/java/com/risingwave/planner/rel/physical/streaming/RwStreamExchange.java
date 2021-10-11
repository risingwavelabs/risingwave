package com.risingwave.planner.rel.physical.streaming;

import static com.google.common.base.Verify.verify;

import com.risingwave.planner.rel.common.dist.RwDistributionTrait;
import com.risingwave.proto.streaming.plan.StreamNode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Exchange;

public class RwStreamExchange extends Exchange implements RisingWaveStreamingRel {

  protected RwStreamExchange(
      RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelDistribution distribution) {
    super(cluster, traitSet, input, distribution);
    checkConvention();
    verify(
        traitSet.contains(distribution), "Trait set: %s, distribution: %s", traitSet, distribution);
  }

  @Override
  public StreamNode serialize() {
    // This node should never be serialized.
    return null;
  }

  @Override
  public Exchange copy(RelTraitSet traitSet, RelNode newInput, RelDistribution newDistribution) {
    return new RwStreamExchange(getCluster(), traitSet, newInput, newDistribution);
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

  public static RwStreamExchange create(RelNode input, RwDistributionTrait distribution) {
    RelOptCluster cluster = input.getCluster();
    RelTraitSet traitSet = input.getTraitSet().plus(STREAMING).plus(distribution);
    var dist = traitSet.canonize(distribution);
    return new RwStreamExchange(cluster, traitSet, input, dist);
  }
}
