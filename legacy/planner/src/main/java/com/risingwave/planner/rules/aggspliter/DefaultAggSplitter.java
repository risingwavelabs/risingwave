package com.risingwave.planner.rules.aggspliter;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexBuilder;

class DefaultAggSplitter extends AbstractAggSplitter {
  protected DefaultAggSplitter(
      RexBuilder rexBuilder,
      int groupCount,
      AggregateCall originalAggCall,
      int originalAggCallIndex) {
    super(rexBuilder, groupCount, originalAggCall, originalAggCallIndex);
  }
}
