package com.risingwave.planner.rules.logical;

import com.risingwave.planner.rel.logical.RisingWaveLogicalRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.checkerframework.checker.nullness.qual.Nullable;

public class LogicalCalcConverterRule extends ConverterRule {
  public static final LogicalCalcConverterRule INSTANCE =
      new LogicalCalcConverterRule(
          Config.INSTANCE.withConversion(
              LogicalCalc.class,
              Convention.NONE,
              RisingWaveLogicalRel.LOGICAL,
              "Converting calcite logical calc to risingwave"));

  private LogicalCalcConverterRule(Config config) {
    super(config);
  }

  @Override
  public @Nullable RelNode convert(RelNode rel) {
    if (rel instanceof LogicalCalc) {
      LogicalCalc calciteCalc = (LogicalCalc) rel;
      return new com.risingwave.planner.rel.logical.LogicalCalc(
          rel.getCluster(),
          rel.getTraitSet().replace(RisingWaveLogicalRel.LOGICAL),
          calciteCalc.getHints(),
          calciteCalc.getInput(),
          calciteCalc.getProgram());
    }
    return null;
  }
}
