package com.risingwave.planner.rules;

import com.risingwave.planner.rel.logical.RwLogicalAggregate;
import com.risingwave.planner.rel.logical.RwLogicalFilter;
import com.risingwave.planner.rel.logical.RwLogicalFilterScan;
import com.risingwave.planner.rel.logical.RwLogicalInsert;
import com.risingwave.planner.rel.logical.RwLogicalProject;
import com.risingwave.planner.rel.logical.RwLogicalValues;
import com.risingwave.planner.rel.physical.batch.RwBatchFilter;
import com.risingwave.planner.rel.physical.batch.RwBatchFilterScan;
import com.risingwave.planner.rel.physical.batch.RwBatchGather;
import com.risingwave.planner.rel.physical.batch.RwBatchHashAgg;
import com.risingwave.planner.rel.physical.batch.RwBatchProject;
import com.risingwave.planner.rel.physical.batch.RwBatchValues;
import com.risingwave.planner.rules.logical.ProjectToTableScanRule;
import com.risingwave.planner.rules.physical.batch.BatchInsertValuesRule;
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
          RwLogicalInsert.LogicalInsertConverterRule.INSTANCE,
          RwLogicalProject.RwProjectConverterRule.INSTANCE,
          RwLogicalFilter.RwFilterConverterRule.INSTANCE,
          RwLogicalAggregate.RwAggregateConverterRule.INSTANCE,
          RwLogicalValues.RwValuesConverterRule.INSTANCE,
          RwLogicalFilterScan.RwLogicalFilterScanConverterRule.INSTANCE);

  public static final RuleSet LOGICAL_OPTIMIZATION_RULES =
      RuleSets.ofList(ProjectToTableScanRule.Config.INSTANCE.toRule());

  public static final RuleSet PHYSICAL_RULES =
      RuleSets.ofList(
          AbstractConverter.ExpandConversionRule.Config.DEFAULT.toRule(),
          RwBatchFilter.BatchFilterConverterRule.INSTANCE,
          RwBatchProject.BatchProjectConverterRule.INSTANCE,
          RwBatchHashAgg.BatchHashAggConverterRule.INSTANCE,
          RwBatchValues.BatchValuesConverterRule.INSTANCE,
          BatchInsertValuesRule.Config.DEFAULT.toRule(),
          RwBatchFilterScan.BatchFilterScanConverterRule.INSTANCE,
          RwBatchGather.RwBatchGatherConverterRule.INSTANCE);
}
