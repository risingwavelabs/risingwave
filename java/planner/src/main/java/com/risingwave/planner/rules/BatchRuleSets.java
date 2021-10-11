package com.risingwave.planner.rules;

import com.risingwave.planner.rel.logical.RwLogicalAggregate;
import com.risingwave.planner.rel.logical.RwLogicalFilter;
import com.risingwave.planner.rel.logical.RwLogicalInsert;
import com.risingwave.planner.rel.logical.RwLogicalJoin;
import com.risingwave.planner.rel.logical.RwLogicalProject;
import com.risingwave.planner.rel.logical.RwLogicalScan;
import com.risingwave.planner.rel.logical.RwLogicalSort;
import com.risingwave.planner.rel.logical.RwLogicalValues;
import com.risingwave.planner.rel.physical.batch.RwBatchFilter;
import com.risingwave.planner.rel.physical.batch.RwBatchGather;
import com.risingwave.planner.rel.physical.batch.RwBatchInsertValues;
import com.risingwave.planner.rel.physical.batch.RwBatchProject;
import com.risingwave.planner.rel.physical.batch.RwBatchSort;
import com.risingwave.planner.rel.physical.batch.RwBatchValues;
import com.risingwave.planner.rules.logical.GatherConversionRule;
import com.risingwave.planner.rules.logical.ProjectToTableScanRule;
import com.risingwave.planner.rules.physical.batch.BatchExpandConverterRule;
import com.risingwave.planner.rules.physical.batch.BatchScanConverterRule;
import com.risingwave.planner.rules.physical.batch.InsertValuesRule;
import com.risingwave.planner.rules.physical.batch.aggregate.BatchHashAggRule;
import com.risingwave.planner.rules.physical.batch.aggregate.BatchSortAggRule;
import com.risingwave.planner.rules.physical.batch.join.BatchHashJoinRule;
import com.risingwave.planner.rules.physical.batch.join.BatchNestedLoopJoinRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

public class BatchRuleSets {
  private BatchRuleSets() {}

  public static final RuleSet SUB_QUERY_REWRITE_RULES =
      RuleSets.ofList(
          CoreRules.FILTER_SUB_QUERY_TO_CORRELATE,
          CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
          CoreRules.JOIN_SUB_QUERY_TO_CORRELATE);

  public static final RuleSet LOGICAL_REWRITE_RULES =
      RuleSets.ofList(
          CoreRules.UNION_TO_DISTINCT,
          CoreRules.FILTER_INTO_JOIN,
          CoreRules.JOIN_CONDITION_PUSH,
          CoreRules.JOIN_PUSH_EXPRESSIONS,
          CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES,

          // Don't put these three reduce rules in cbo, since they prunes matched rel nodes
          // in planner and disable further optimization.
          CoreRules.FILTER_REDUCE_EXPRESSIONS,
          CoreRules.PROJECT_REDUCE_EXPRESSIONS,
          CoreRules.JOIN_REDUCE_EXPRESSIONS,
          CoreRules.FILTER_MERGE,
          CoreRules.PROJECT_MERGE,
          CoreRules.PROJECT_REMOVE,
          CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS,
          CoreRules.SORT_REMOVE,
          CoreRules.FILTER_EXPAND_IS_NOT_DISTINCT_FROM,
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

  public static final RuleSet LOGICAL_OPTIMIZE_RULES =
      RuleSets.ofList(
          CoreRules.UNION_TO_DISTINCT,
          CoreRules.FILTER_INTO_JOIN,
          CoreRules.JOIN_CONDITION_PUSH,
          CoreRules.JOIN_PUSH_EXPRESSIONS,
          CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES,
          CoreRules.FILTER_AGGREGATE_TRANSPOSE,
          CoreRules.FILTER_PROJECT_TRANSPOSE,
          CoreRules.FILTER_SET_OP_TRANSPOSE,
          CoreRules.FILTER_MERGE,
          CoreRules.PROJECT_FILTER_TRANSPOSE,
          CoreRules.PROJECT_MERGE,
          CoreRules.PROJECT_REMOVE,
          CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS,
          CoreRules.SORT_REMOVE,
          CoreRules.FILTER_EXPAND_IS_NOT_DISTINCT_FROM,
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
          RwLogicalScan.RwLogicalFilterScanConverterRule.INSTANCE,
          RwLogicalSort.RwLogicalSortConverterRule.INSTANCE,
          RwLogicalJoin.RwLogicalJoinConverterRule.INSTANCE,
          GatherConversionRule.Config.DEFAULT.toRule());

  public static final RuleSet LOGICAL_OPTIMIZATION_RULES =
      RuleSets.ofList(
          ProjectToTableScanRule.Config.INSTANCE.toRule(),
          InsertValuesRule.Config.VALUES.toRule(),
          InsertValuesRule.Config.PROJECT_VALUES.toRule());

  public static final RuleSet PHYSICAL_CONVERTER_RULES =
      RuleSets.ofList(
          BatchExpandConverterRule.Config.DEFAULT.toRule(),
          RwBatchFilter.BatchFilterConverterRule.INSTANCE,
          RwBatchProject.BatchProjectConverterRule.INSTANCE,
          RwBatchValues.BatchValuesConverterRule.INSTANCE,
          BatchScanConverterRule.INSTANCE,
          RwBatchGather.RwBatchGatherConverterRule.INSTANCE,
          RwBatchSort.RwBatchSortConverterRule.INSTANCE,
          RwBatchInsertValues.BatchInsertValuesConverterRule.INSTANCE);

  public static final RuleSet PHYSICAL_AGG_RULES =
      RuleSets.ofList(
          BatchHashAggRule.Config.DEFAULT.toRule(), BatchSortAggRule.Config.DEFAULT.toRule());

  public static final RuleSet PHYSICAL_JOIN_RULES =
      RuleSets.ofList(
          BatchHashJoinRule.Config.DEFAULT.toRule(),
          BatchNestedLoopJoinRule.Config.DEFAULT.toRule());
}
