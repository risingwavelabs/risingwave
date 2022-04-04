package com.risingwave.planner.rules.aggspliter;

import static com.google.common.base.Verify.verify;

import com.google.common.collect.ImmutableList;
import java.util.stream.IntStream;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;

public abstract class AbstractAggSplitter implements AggSplitter {
  protected final RexBuilder rexBuilder;
  protected final int groupCount;
  protected final AggregateCall originalAggCall;
  protected ImmutableIntList prevStageIndexes;
  protected State state;

  protected AbstractAggSplitter(
      RexBuilder rexBuilder,
      int groupCount,
      AggregateCall originalAggCall,
      int originalAggCallIndex) {
    this.rexBuilder = rexBuilder;
    this.groupCount = groupCount;
    this.originalAggCall = originalAggCall;
    this.prevStageIndexes = ImmutableIntList.of(originalAggCallIndex);
    this.state = State.INITED;
  }

  private enum State {
    INITED,
    LOCAL_SPLIT_DONE,
    GLOBAL_SPLIT_DONE,
    GLOBAL_CALC_DONE
  }

  @Override
  public RexNode makeLastCalc(SplitterArgs args) {
    checkState(State.GLOBAL_SPLIT_DONE);
    RexNode ret = doMakeLastCalc(args);
    state = State.GLOBAL_CALC_DONE;
    return ret;
  }

  protected RexNode doMakeLastCalc(SplitterArgs args) {
    return rexBuilder.makeInputRef(args.getInput(), prevStageIndexes.get(0));
  }

  @Override
  public ImmutableList<AggregateCall> makeLocalAggCall(SplitterArgs args) {
    checkState(State.INITED);
    var ret = doMakeLocalAggCall(args);
    prevStageIndexes =
        ImmutableIntList.of(
            IntStream.range(args.getStartIndex(), args.getStartIndex() + ret.size()).toArray());
    state = State.LOCAL_SPLIT_DONE;
    return ret;
  }

  protected ImmutableList<AggregateCall> doMakeLocalAggCall(SplitterArgs args) {
    return ImmutableList.of(originalAggCall);
  }

  @Override
  public ImmutableList<AggregateCall> makeGlobalAggCall(SplitterArgs args) {
    checkState(State.LOCAL_SPLIT_DONE);
    var ret = doMakeGlobalAggCall(args);
    prevStageIndexes =
        ImmutableIntList.of(
            IntStream.range(args.getStartIndex(), args.getStartIndex() + ret.size()).toArray());
    state = State.GLOBAL_SPLIT_DONE;
    return ret;
  }

  protected ImmutableList<AggregateCall> doMakeGlobalAggCall(SplitterArgs args) {
    var newAggCall =
        originalAggCall.adaptTo(
            args.getInput(), prevStageIndexes, originalAggCall.filterArg, groupCount, groupCount);
    return ImmutableList.of(newAggCall);
  }

  @Override
  public boolean isLastCalcTrivial() {
    return true;
  }

  private void checkState(State expected) {
    verify(state == expected, "Invalid state: %s, expected: %s", state, expected);
  }
}
