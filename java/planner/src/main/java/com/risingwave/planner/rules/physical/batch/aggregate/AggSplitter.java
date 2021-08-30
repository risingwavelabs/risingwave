package com.risingwave.planner.rules.physical.batch.aggregate;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexNode;

/**
 * Split agg call to two phase agg call in distributed environment.
 *
 * <p>For example, it will split {@code avg(a)} to following calls:
 *
 * <ul>
 *   <li>Local agg: {@code sum(a), count(*) as 1}
 *   <li>Global agg: {@code sum(a), sum(*)}
 *   <li>Last calc: {@code sum(a) / count(*)}
 * </ul>
 */
public interface AggSplitter {
  class SplitterArgs {
    private final int startIndex;
    private final RelNode input;

    public SplitterArgs(int startIndex, RelNode input) {
      this.startIndex = startIndex;
      this.input = input;
    }

    public RelNode getInput() {
      return input;
    }

    public int getStartIndex() {
      return startIndex;
    }
  }

  ImmutableList<AggregateCall> makeLocalAggCall(SplitterArgs args);

  ImmutableList<AggregateCall> makeGlobalAggCall(SplitterArgs args);

  RexNode makeLastCalc(SplitterArgs args);

  default boolean isLastCalcTrivial() {
    return true;
  }
}
