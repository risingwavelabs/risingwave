package com.risingwave.planner.rules;

import com.risingwave.planner.rules.logical.BatchFilterScanRule;
import com.risingwave.planner.rules.logical.CalcToScanRule;
import com.risingwave.planner.rules.logical.LogicalCalcConverterRule;
import com.risingwave.planner.rules.physical.batch.BatchPhysicalFilterScanRule;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

public class BatchRuleSets {
  private BatchRuleSets() {}

  public static final RuleSet CALCITE_LOGICAL_OPTIMIZE_RULES =
      RuleSets.ofList(ProjectToCalcRule.Config.DEFAULT.toRule());

  public static final RuleSet LOGICAL_CONVERSION_RULES =
      RuleSets.ofList(
          CalcToScanRule.Config.DEFAULT.toRule(),
          LogicalCalcConverterRule.INSTANCE,
          BatchFilterScanRule.Config.DEFAULT.toRule());

  public static final RuleSet PHYSICAL_RULES =
      RuleSets.ofList(
          AbstractConverter.ExpandConversionRule.INSTANCE,
          BatchPhysicalFilterScanRule.Config.DEFAULT.toRule());
}
