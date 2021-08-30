package com.risingwave.planner.rules.physical.batch.aggregate;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Aggregate;

public class AggSplitters {
  public static AggSplitter from(RelOptRuleCall optCall, Aggregate agg, int aggCallIdx) {
    var rexBuilder = optCall.rel(0).getCluster().getRexBuilder();
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
