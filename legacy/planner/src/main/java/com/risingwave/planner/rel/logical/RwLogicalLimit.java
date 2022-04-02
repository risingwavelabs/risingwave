package com.risingwave.planner.rel.logical;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Logical limit operator in RisingWave */
public class RwLogicalLimit extends SingleRel implements RisingWaveLogicalRel {
  public final @Nullable RexNode offset;
  public final @Nullable RexNode fetch;

  public RwLogicalLimit(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      @Nullable RexNode offset,
      @Nullable RexNode fetch) {
    super(cluster, traitSet, input);
    this.offset = offset;
    this.fetch = fetch;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new RwLogicalLimit(this.getCluster(), traitSet, sole(inputs), offset, fetch);
  }
}
