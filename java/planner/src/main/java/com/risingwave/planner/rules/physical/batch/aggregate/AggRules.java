package com.risingwave.planner.rules.physical.batch.aggregate;

import com.google.common.collect.Streams;
import com.risingwave.planner.rel.logical.RwLogicalAggregate;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;

public class AggRules {
  static List<AggSplitter> getAggSplitters(RwLogicalAggregate logicalAgg, RelOptRuleCall call) {
    return Streams.mapWithIndex(
            logicalAgg.getAggCallList().stream(),
            (aggCall, idx) ->
                AggSplitters.from(call, logicalAgg, (int) (idx + logicalAgg.getGroupCount())))
        .collect(Collectors.toList());
  }

  static List<List<AggregateCall>> getLocalAggCalls(
      RwLogicalAggregate logicalAgg, List<AggSplitter> splitters) {
    var start = logicalAgg.getGroupCount();
    var ret = new ArrayList<List<AggregateCall>>(splitters.size());
    for (var splitter : splitters) {
      var arg = new AggSplitter.SplitterArgs(start, logicalAgg.getInput());
      var localAggs = splitter.makeLocalAggCall(arg);
      start += localAggs.size();
      ret.add(localAggs);
    }

    return ret;
  }

  static List<List<AggregateCall>> getGlobalAggCalls(
      RwLogicalAggregate logicalAgg, RelNode newInput, List<AggSplitter> splitters) {
    var start = logicalAgg.getGroupCount();
    var ret = new ArrayList<List<AggregateCall>>(splitters.size());
    for (var splitter : splitters) {
      var arg = new AggSplitter.SplitterArgs(start, newInput);
      var globalAggCalls = splitter.makeGlobalAggCall(arg);
      start += globalAggCalls.size();
      ret.add(globalAggCalls);
    }

    return ret;
  }

  static List<RexNode> getLastCalcs(
      RwLogicalAggregate logicalAgg, RelNode input, List<AggSplitter> splitters) {
    var start = logicalAgg.getGroupCount();
    var ret = new ArrayList<RexNode>(splitters.size());

    for (var splitter : splitters) {
      var arg = new AggSplitter.SplitterArgs(start, input);
      ret.add(splitter.makeLastCalc(arg));
      start += 1;
    }

    return ret;
  }

  static RelCollation collationOf(ImmutableBitSet groupSet) {
    return RelCollations.of(ImmutableIntList.copyOf(groupSet.asList()));
  }
}
