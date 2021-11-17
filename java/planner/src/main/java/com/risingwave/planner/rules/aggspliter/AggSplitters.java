package com.risingwave.planner.rules.aggspliter;

import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rex.RexBuilder;

public class AggSplitters {
  public static AggSplitter from(RexBuilder rexBuilder, Aggregate agg, int aggCallIdx) {
    var groupCount = agg.getGroupCount();
    var originalAggCall = agg.getAggCallList().get(aggCallIdx - groupCount);
    switch (originalAggCall.getAggregation().getKind()) {
      case AVG:
        return new AvgSplitter(rexBuilder, groupCount, originalAggCall, aggCallIdx);
      case COUNT:
        return new CountSplitter(rexBuilder, groupCount, originalAggCall, aggCallIdx);
      default:
        return new DefaultAggSplitter(rexBuilder, groupCount, originalAggCall, aggCallIdx);
    }
  }
}
