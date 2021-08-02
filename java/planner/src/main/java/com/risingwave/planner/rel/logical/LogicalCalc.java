package com.risingwave.planner.rel.logical;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexProgram;

public class LogicalCalc extends Calc implements RisingWaveLogicalRel {
  public LogicalCalc(
      RelOptCluster cluster,
      RelTraitSet traits,
      List<RelHint> hints,
      RelNode child,
      RexProgram program) {
    super(cluster, traits, hints, child, program);
    checkArgument(traitSet.contains(RisingWaveLogicalRel.LOGICAL), "Not logical convention.");
  }

  @Override
  public LogicalCalc copy(RelTraitSet traitSet, RelNode child, RexProgram program) {
    return new LogicalCalc(getCluster(), traitSet, hints, child, program);
  }

  public boolean canBePushedToScan() {
    return (program.getCondition() == null)
        && program.getExprList().stream().allMatch(rex -> rex instanceof RexInputRef);
  }
}
