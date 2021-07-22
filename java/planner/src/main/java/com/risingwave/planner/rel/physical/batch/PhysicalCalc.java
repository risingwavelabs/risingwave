package com.risingwave.planner.rel.physical.batch;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexProgram;

public class PhysicalCalc extends Calc {
  protected PhysicalCalc(
      RelOptCluster cluster,
      RelTraitSet traits,
      List<RelHint> hints,
      RelNode child,
      RexProgram program) {
    super(cluster, traits, hints, child, program);
    checkArgument(traitSet.contains(RisingWaveBatchPhyRel.BATCH_PHYSICAL));
  }

  @Override
  public PhysicalCalc copy(RelTraitSet traitSet, RelNode child, RexProgram program) {
    return new PhysicalCalc(getCluster(), traitSet, getHints(), child, program);
  }
}
