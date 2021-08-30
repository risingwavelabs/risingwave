package com.risingwave.planner.rules;

import com.risingwave.planner.rel.logical.RwLogicalAggregate;
import com.risingwave.planner.rel.logical.RwLogicalFilter;
import com.risingwave.planner.rel.logical.RwLogicalFilterScan;
import com.risingwave.planner.rel.logical.RwLogicalInsert;
import com.risingwave.planner.rel.logical.RwLogicalProject;
import com.risingwave.planner.rel.logical.RwLogicalSort;
import com.risingwave.planner.rel.logical.RwLogicalValues;
import com.risingwave.planner.rel.physical.batch.RwBatchFilter;
import com.risingwave.planner.rel.physical.batch.RwBatchFilterScan;
import com.risingwave.planner.rel.physical.batch.RwBatchGather;
import com.risingwave.planner.rel.physical.batch.RwBatchProject;
import com.risingwave.planner.rel.physical.batch.RwBatchSort;
import com.risingwave.planner.rel.physical.batch.RwBatchValues;
import com.risingwave.planner.rules.logical.ProjectToTableScanRule;
import com.risingwave.planner.rules.physical.batch.BatchExpandConverterRule;
import com.risingwave.planner.rules.physical.batch.BatchInsertValuesRule;
import com.risingwave.planner.rules.physical.batch.aggregate.BatchHashAggRule;
import com.risingwave.planner.rules.physical.batch.aggregate.BatchSortAggRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

public class BatchRuleSets {
  private BatchRuleSets() {}

  public static final RuleSet CALCITE_LOGICAL_OPTIMIZE_RULES =
      RuleSets.ofList(
          CoreRules.PROJECT_FILTER_TRANSPOSE,
          PruneEmptyRules.UNION_INSTANCE,
          PruneEmptyRules.INTERSECT_INSTANCE,
          PruneEmptyRules.MINUS_INSTANCE,
          PruneEmptyRules.PROJECT_INSTANCE,
          PruneEmptyRules.FILTER_INSTANCE,
          PruneEmptyRules.SORT_INSTANCE,
          PruneEmptyRules.AGGREGATE_INSTANCE,
          PruneEmptyRules.JOIN_LEFT_INSTANCE,
          PruneEmptyRules.JOIN_RIGHT_INSTANCE,
          PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE);

  public static final RuleSet LOGICAL_CONVERTER_RULES =
      RuleSets.ofList(
          RwLogicalInsert.LogicalInsertConverterRule.INSTANCE,
          RwLogicalProject.RwProjectConverterRule.INSTANCE,
          RwLogicalFilter.RwFilterConverterRule.INSTANCE,
          RwLogicalAggregate.RwAggregateConverterRule.INSTANCE,
          RwLogicalValues.RwValuesConverterRule.INSTANCE,
          RwLogicalFilterScan.RwLogicalFilterScanConverterRule.INSTANCE,
          RwLogicalSort.RwLogicalSortConverterRule.INSTANCE);

  public static final RuleSet LOGICAL_OPTIMIZATION_RULES =
      RuleSets.ofList(ProjectToTableScanRule.Config.INSTANCE.toRule());

  public static final RuleSet PHYSICAL_CONVERTER_RULES =
      RuleSets.ofList(
          //          AbstractConverter.ExpandConversionRule.Config.DEFAULT.toRule(),
          BatchExpandConverterRule.Config.DEFAULT.toRule(),
          RwBatchFilter.BatchFilterConverterRule.INSTANCE,
          RwBatchProject.BatchProjectConverterRule.INSTANCE,
          RwBatchValues.BatchValuesConverterRule.INSTANCE,
          BatchInsertValuesRule.Config.VALUES.toRule(),
          BatchInsertValuesRule.Config.PROJECT_VALUES.toRule(),
          RwBatchFilterScan.BatchFilterScanConverterRule.INSTANCE,
          RwBatchGather.RwBatchGatherConverterRule.INSTANCE,
          RwBatchSort.RwBatchSortConverterRule.INSTANCE);

  public static final RuleSet PHYSICAL_AGG_RULES =
      RuleSets.ofList(
          BatchHashAggRule.Config.DEFAULT.toRule(), BatchSortAggRule.Config.DEFAULT.toRule());
}
