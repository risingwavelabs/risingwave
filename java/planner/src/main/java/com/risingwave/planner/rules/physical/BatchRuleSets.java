package com.risingwave.planner.rules.physical;

import com.risingwave.planner.rel.logical.*;
import com.risingwave.planner.rel.physical.*;
import com.risingwave.planner.rel.physical.join.RwBatchHashJoin;
import com.risingwave.planner.rel.physical.join.RwBatchNestedLoopJoin;
import com.risingwave.planner.rel.physical.join.RwBatchSortMergeJoin;
import com.risingwave.planner.rules.distributed.agg.ShuffleAggRule;
import com.risingwave.planner.rules.distributed.agg.SingleLimitRule;
import com.risingwave.planner.rules.distributed.agg.TwoPhaseAggRule;
import com.risingwave.planner.rules.distributed.agg.TwoPhaseLimitRule;
import com.risingwave.planner.rules.distributed.join.BroadcastJoinRule;
import com.risingwave.planner.rules.distributed.join.ShuffleJoinRule;
import com.risingwave.planner.rules.logical.ProjectToTableScanRule;
import com.risingwave.planner.rules.logical.SimpleCountStarColumnPruningRule;
import com.risingwave.planner.rules.logical.subquery.FlinkSubQueryRemoveRule;
import com.risingwave.planner.rules.logical.subquery.JoinConditionTypeCoerceRule;
import com.risingwave.planner.rules.logical.subquery.JoinPushExpressionsRule;
import com.risingwave.planner.rules.logical.subquery.RewriteSubQueryRule;
import com.risingwave.planner.rules.logical.subquery.SimplifyFilterConditionRule;
import org.apache.calcite.rel.rules.AggregateExtractProjectRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

/** Planner rule sets. */
public class BatchRuleSets {
  private BatchRuleSets() {}

  public static final RuleSet NEW_SUB_QUERY_RULES =
      RuleSets.ofList(
          SimplifyFilterConditionRule.INSTANCE,
          RewriteSubQueryRule.INSTANCE,
          FlinkSubQueryRemoveRule.SINGLETON,
          JoinConditionTypeCoerceRule.INSTANCE,
          JoinPushExpressionsRule.INSTANCE);

  public static final RuleSet SUB_QUERY_REWRITE_RULES =
      RuleSets.ofList(
          CoreRules.FILTER_SUB_QUERY_TO_CORRELATE,
          CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
          CoreRules.JOIN_SUB_QUERY_TO_CORRELATE);

  public static final RuleSet LOGICAL_REWRITE_RULES =
      RuleSets.ofList(
          AggregateExtractProjectRule.SCAN,
          CoreRules.UNION_TO_DISTINCT,
          CoreRules.FILTER_INTO_JOIN,
          CoreRules.JOIN_CONDITION_PUSH,
          CoreRules.JOIN_PUSH_EXPRESSIONS,
          CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES,

          // Don't put these three reduce rules in cbo, since they prunes matched rel nodes
          // in planner and disable further optimization.
          // CoreRules.FILTER_REDUCE_EXPRESSIONS,
          // CoreRules.PROJECT_REDUCE_EXPRESSIONS,
          // CoreRules.JOIN_REDUCE_EXPRESSIONS,
          CoreRules.FILTER_MERGE,
          CoreRules.PROJECT_MERGE,
          CoreRules.PROJECT_REMOVE,
          CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS,
          CoreRules.SORT_REMOVE,
          CoreRules.FILTER_EXPAND_IS_NOT_DISTINCT_FROM,
          ProjectJoinTransposeRule.Config.DEFAULT.toRule(),
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
          RwLogicalDelete.LogicalDeleteConverterRule.INSTANCE,
          RwLogicalProject.RwProjectConverterRule.INSTANCE,
          RwLogicalFilter.RwFilterConverterRule.INSTANCE,
          RwLogicalAggregate.RwBatchAggregateConverterRule.INSTANCE,
          RwLogicalValues.RwValuesConverterRule.INSTANCE,
          RwLogicalValues.RwValuesUnionConverterRule.INSTANCE,
          RwLogicalScan.RwLogicalScanConverterRule.INSTANCE,
          RwLogicalSort.RwLogicalSortConverterRule.INSTANCE,
          RwLogicalJoin.RwLogicalJoinConverterRule.INSTANCE,
          RwLogicalGenerateSeries.RwGenerateSeriesConverterRule.INSTANCE);

  public static final RuleSet LOGICAL_OPTIMIZATION_RULES =
      RuleSets.ofList(
          ProjectToTableScanRule.Config.INSTANCE.toRule(),
          SimpleCountStarColumnPruningRule.Config.INSTANCE.toRule());

  public static final RuleSet PHYSICAL_CONVERTER_RULES =
      RuleSets.ofList(
          RwBatchFilter.BatchFilterConverterRule.INSTANCE,
          RwBatchProject.BatchProjectConverterRule.INSTANCE,
          BatchScanConverterRule.INSTANCE,
          RwBatchSort.RwBatchSortConverterRule.INSTANCE,
          RwBatchInsert.BatchInsertConverterRule.INSTANCE,
          RwBatchDelete.BatchDeleteConverterRule.INSTANCE,
          RwBatchValues.BatchValuesConverterRule.INSTANCE,
          RwBatchHashJoin.BatchHashJoinConverterRule.INSTANCE,
          RwBatchNestedLoopJoin.BatchNestedLoopJoinConverterRule.INSTANCE,
          RwBatchSortMergeJoin.BatchSortMergeJoinConverterRule.ASC,
          RwBatchSortMergeJoin.BatchSortMergeJoinConverterRule.DESC,
          RwBatchHashAgg.BatchHashAggConverterRule.INSTANCE,
          RwBatchSortAgg.BatchSortAggConverterRule.INSTANCE,
          RwBatchGenerateSeries.BatchGenerateSeriesConverterRule.INSTANCE,
          RwBatchLimit.BatchLimitConverterRule.INSTANCE,
          CoreRules.SORT_REMOVE);

  public static final RuleSet DISTRIBUTED_CONVERTER_RULES =
      RuleSets.ofList(
          RisingWaveBatchPhyRel.getDistributedConvertRule(RwBatchFilter.class),
          RisingWaveBatchPhyRel.getDistributedConvertRule(RwBatchProject.class),
          RisingWaveBatchPhyRel.getDistributedConvertRule(RwBatchSort.class),
          RisingWaveBatchPhyRel.getDistributedConvertRule(RwBatchValues.class),
          RisingWaveBatchPhyRel.getDistributedConvertRule(RwBatchInsert.class),
          RisingWaveBatchPhyRel.getDistributedConvertRule(RwBatchDelete.class),
          RisingWaveBatchPhyRel.getDistributedConvertRule(RwBatchSourceScan.class),
          RisingWaveBatchPhyRel.getDistributedConvertRule(RwBatchScan.class),
          RisingWaveBatchPhyRel.getDistributedConvertRule(RwBatchGenerateSeries.class));

  public static final RuleSet DISTRIBUTION_RULES =
      RuleSets.ofList(
          BroadcastJoinRule.INSTANCE,
          ShuffleJoinRule.INSTANCE,
          // FIXME: currently cardinality estimation is inaccurate without enough statistics to
          // determine shuffleAgg or 2phaseAgg
          ShuffleAggRule.INSTANCE,
          TwoPhaseAggRule.INSTANCE,
          TwoPhaseLimitRule.INSTANCE,
          SingleLimitRule.INSTANCE);
}
