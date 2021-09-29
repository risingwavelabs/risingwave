package com.risingwave.planner.rules.streaming;

import com.risingwave.planner.rel.physical.streaming.RwStreamAgg;
import com.risingwave.planner.rel.physical.streaming.RwStreamFilter;
import com.risingwave.planner.rel.physical.streaming.RwStreamProject;
import com.risingwave.planner.rel.physical.streaming.RwStreamTableSource;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

public class StreamingConvertRules {
  public StreamingConvertRules() {}

  public static final RuleSet STREAMING_CONVERTER_RULES =
      RuleSets.ofList(
          RwStreamFilter.StreamFilterConverterRule.INSTANCE,
          RwStreamProject.StreamProjectConverterRule.INSTANCE,
          RwStreamTableSource.StreamTableSourceConverterRule.INSTANCE,
          RwStreamAgg.StreamAggregationConverterRule.INSTANCE);
}
