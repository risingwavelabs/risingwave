package com.risingwave.planner.rules.streaming;

import com.risingwave.planner.rel.physical.streaming.RwStreamFilter;
import com.risingwave.planner.rel.physical.streaming.RwStreamProject;
import com.risingwave.planner.rel.physical.streaming.RwStreamTableSource;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

/** Rules for converting logical RelNode to stream RelNode */
public class StreamingConvertRules {
  public StreamingConvertRules() {}

  public static final RuleSet STREAMING_CONVERTER_RULES =
      RuleSets.ofList(
          RwStreamFilter.StreamFilterConverterRule.INSTANCE,
          RwStreamProject.StreamProjectConverterRule.INSTANCE,
          RwStreamTableSource.StreamTableSourceConverterRule.INSTANCE,
          StreamingAggRule.Config.DEFAULT.toRule(),
          StreamingExpandConverterRule.Config.DEFAULT.toRule(),
          StreamingEliminateProjectRule.Config.DEFAULT.toRule());
}
