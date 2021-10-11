package com.risingwave.planner.rules.streaming;

import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

public class StreamingParallelPipelineRules {
  public StreamingParallelPipelineRules() {}

  public static final RuleSet STREAMING_PARALLEL_RULES =
      RuleSets.ofList(StreamingSourceDispatchRule.Config.DEFAULT.toRule());
}
