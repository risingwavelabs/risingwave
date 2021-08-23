package com.risingwave.planner.rel.logical;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

/**
 * Relational operator to gather inputs and send to frontend console.
 *
 * <p>The parallelism is always 1, and its distribution type is always single.
 */
public class RwLogicalGather extends SingleRel implements RisingWaveLogicalRel {
  private RwLogicalGather(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
    super(cluster, traits, input);
    checkConvention();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new RwLogicalGather(getCluster(), traitSet, sole(inputs));
  }

  public static RwLogicalGather create(RelNode input) {
    RelTraitSet newTraits = input.getTraitSet().plus(LOGICAL);
    return new RwLogicalGather(input.getCluster(), newTraits, input);
  }
}
