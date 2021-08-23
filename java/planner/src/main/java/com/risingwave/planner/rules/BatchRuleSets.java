package com.risingwave.planner.rules;

import com.risingwave.planner.rel.logical.RwAggregate;
import com.risingwave.planner.rel.logical.RwFilter;
import com.risingwave.planner.rel.logical.RwInsert;
import com.risingwave.planner.rel.logical.RwProject;
import com.risingwave.planner.rel.logical.RwValues;
import com.risingwave.planner.rel.physical.batch.BatchFilter;
import com.risingwave.planner.rel.physical.batch.BatchHashAgg;
import com.risingwave.planner.rel.physical.batch.BatchProject;
import com.risingwave.planner.rel.physical.batch.BatchValues;
import com.risingwave.planner.rel.physical.batch.RwBatchGather;
import com.risingwave.planner.rules.logical.BatchFilterScanRule;
import com.risingwave.planner.rules.logical.ProjectToTableScanRule;
import com.risingwave.planner.rules.physical.batch.BatchPhysicalFilterScanRule;
import com.risingwave.planner.rules.physical.batch.BatchPhysicalInsertValuesRule;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

public class BatchRuleSets {
  private BatchRuleSets() {}

  public static final RuleSet CALCITE_LOGICAL_OPTIMIZE_RULES =
      RuleSets.ofList(CoreRules.PROJECT_FILTER_TRANSPOSE);

  public static final RuleSet LOGICAL_CONVERSION_RULES =
      RuleSets.ofList(
          RwInsert.LogicalInsertConverterRule.INSTANCE,
          RwProject.RwProjectConverterRule.INSTANCE,
          RwFilter.RwFilterConverterRule.INSTANCE,
          RwAggregate.RwAggregateConverterRule.INSTANCE,
          RwValues.RwValuesConverterRule.INSTANCE,
          BatchFilterScanRule.Config.DEFAULT.toRule());

  public static final RuleSet LOGICAL_OPTIMIZATION_RULES =
      RuleSets.ofList(ProjectToTableScanRule.Config.INSTANCE.toRule());

  public static final RuleSet PHYSICAL_RULES =
      RuleSets.ofList(
          AbstractConverter.ExpandConversionRule.Config.DEFAULT.toRule(),
          BatchFilter.BatchFilterConverterRule.INSTANCE,
          BatchProject.BatchProjectConverterRule.INSTANCE,
          BatchHashAgg.BatchHashAggConverterRule.INSTANCE,
          BatchValues.BatchValuesConverterRule.INSTANCE,
          BatchPhysicalInsertValuesRule.Config.DEFAULT.toRule(),
          BatchPhysicalFilterScanRule.Config.DEFAULT.toRule(),
          RwBatchGather.RwBatchGatherConverterRule.INSTANCE);
}
