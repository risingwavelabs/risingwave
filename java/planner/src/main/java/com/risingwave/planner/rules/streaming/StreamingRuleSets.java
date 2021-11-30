package com.risingwave.planner.rules.streaming;

import static org.apache.calcite.rel.rules.CoreRules.AGGREGATE_PROJECT_MERGE;

import com.risingwave.planner.rel.streaming.RwStreamFilter;
import com.risingwave.planner.rel.streaming.RwStreamProject;
import com.risingwave.planner.rel.streaming.RwStreamSort;
import com.risingwave.planner.rel.streaming.RwStreamTableSource;
import com.risingwave.planner.rel.streaming.join.RwStreamHashJoin;
import com.risingwave.planner.rules.streaming.aggregate.StreamingShuffleAggRule;
import com.risingwave.planner.rules.streaming.aggregate.StreamingSingleModeAggRule;
import com.risingwave.planner.rules.streaming.aggregate.StreamingTwoPhaseAggRule;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

/** Rules for converting logical RelNode to stream RelNode */
public class StreamingRuleSets {
  public StreamingRuleSets() {}

  public static final RuleSet STREAMING_CONVERTER_RULES =
      RuleSets.ofList(
          RwStreamFilter.StreamFilterConverterRule.INSTANCE,
          RwStreamProject.StreamProjectConverterRule.INSTANCE,
          RwStreamTableSource.StreamTableSourceConverterRule.INSTANCE,
          RwStreamHashJoin.StreamHashJoinConverterRule.INSTANCE,
          RwStreamSort.StreamSortConverterRule.INSTANCE,
          StreamingExpandConverterRule.Config.DEFAULT.toRule());

  public static final RuleSet STREAMING_AGG_RULES =
      RuleSets.ofList(
          StreamingTwoPhaseAggRule.Config.DEFAULT.toRule(),
          StreamingShuffleAggRule.Config.DEFAULT.toRule(),
          StreamingSingleModeAggRule.Config.DEFAULT.toRule());

  // These rules aim to remove, eliminate, merge operators.
  // Since there will be fewer operators, these rules should always improve the cost
  public static final RuleSet STREAMING_REMOVE_RULES =
      RuleSets.ofList(
          StreamingEliminateProjectRule.Config.DEFAULT.toRule(), AGGREGATE_PROJECT_MERGE);
}
