package com.risingwave.planner.rules.distributed.agg;

import com.google.common.collect.Streams;
import com.risingwave.planner.rel.physical.RwAggregate;
import com.risingwave.planner.rules.aggspliter.AggSplitter;
import com.risingwave.planner.rules.aggspliter.AggSplitters;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexNode;

/** Aggregation Helper Functions */
public class SplitUtils {
  /**
   * @param agg Logical Aggregation
   * @return A list of AggSplitter
   */
  public static List<AggSplitter> getAggSplitters(RwAggregate agg) {
    return Streams.mapWithIndex(
            agg.getAggCallList().stream(),
            (aggCall, idx) ->
                AggSplitters.from(
                    agg.getCluster().getRexBuilder(), agg, (int) (idx + agg.getGroupCount())))
        .collect(Collectors.toList());
  }

  /**
   * @param agg Logical Aggregation
   * @param splitters A list of AggSplitter
   * @return A list of lists of AggregateCall
   */
  public static List<List<AggregateCall>> getLocalAggCalls(
      RwAggregate agg, List<AggSplitter> splitters) {
    var start = agg.getGroupCount();
    var ret = new ArrayList<List<AggregateCall>>(splitters.size());
    for (var splitter : splitters) {
      var arg = new AggSplitter.SplitterArgs(start, agg.getInput());
      var localAggs = splitter.makeLocalAggCall(arg);
      start += localAggs.size();
      ret.add(localAggs);
    }

    return ret;
  }

  /**
   * @param agg Logical Aggregation
   * @param newInput The output of local aggregation
   * @param splitters A list of AggSplitter
   * @return A list of lists of AggregateCall
   */
  public static List<List<AggregateCall>> getGlobalAggCalls(
      RwAggregate agg, RelNode newInput, List<AggSplitter> splitters) {
    var start = agg.getGroupCount();
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
   * @param agg Logical Aggregation
   * @param input The input
   * @param splitters A list of AggSplitter
   * @return A list of Expression Node
   */
  public static List<RexNode> getLastCalcs(
      RwAggregate agg, RelNode input, List<AggSplitter> splitters) {
    var start = agg.getGroupCount();
    var ret = new ArrayList<RexNode>(splitters.size());

    for (var splitter : splitters) {
      var arg = new AggSplitter.SplitterArgs(start, input);
      ret.add(splitter.makeLastCalc(arg));
      start += 1;
    }

    return ret;
  }
}
