package com.risingwave.planner.rules.aggspliter;

import com.google.common.collect.Streams;
import com.risingwave.planner.rel.logical.RwLogicalAggregate;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;

/** Aggregation Helper Functions */
// duplicate with com.risingwave.planner.rules.distributed.agg.SplitUtils
// compatible with historical streaming code(StreamingTwoPhaseAggRule)
public class LogicalAggSplitUtils {
  /**
   * @param logicalAgg Logical Aggregation
   * @return A list of AggSplitter
   */
  public static List<AggSplitter> getAggSplitters(RwLogicalAggregate logicalAgg) {
    return Streams.mapWithIndex(
            logicalAgg.getAggCallList().stream(),
            (aggCall, idx) ->
                AggSplitters.from(
                    logicalAgg.getCluster().getRexBuilder(),
                    logicalAgg,
                    (int) (idx + logicalAgg.getGroupCount())))
        .collect(Collectors.toList());
  }

  /**
   * @param logicalAgg Logical Aggregation
   * @param splitters A list of AggSplitter
   * @return A list of lists of AggregateCall
   */
  public static List<List<AggregateCall>> getLocalAggCalls(
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

  /**
   * @param logicalAgg Logical Aggregation
   * @param newInput The output of local aggregation
   * @param splitters A list of AggSplitter
   * @return A list of lists of AggregateCall
   */
  public static List<List<AggregateCall>> getGlobalAggCalls(
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

  /**
   * @param logicalAgg Logical Aggregation
   * @param input The input
   * @param splitters A list of AggSplitter
   * @return A list of Expression Node
   */
  public static List<RexNode> getLastCalcs(
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
