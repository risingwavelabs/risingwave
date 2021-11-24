package com.risingwave.planner.rules.aggspliter;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableIntList;

class AvgSplitter extends AbstractAggSplitter {

  AvgSplitter(
      RexBuilder rexBuilder,
      int groupCount,
      AggregateCall originalAggCall,
      int originalAggCallIndex) {
    super(rexBuilder, groupCount, originalAggCall, originalAggCallIndex);
  }

  @Override
  protected ImmutableList<AggregateCall> doMakeLocalAggCall(SplitterArgs args) {
    var sumAgg =
        AggregateCall.create(
            SqlStdOperatorTable.SUM,
            originalAggCall.isDistinct(),
            originalAggCall.isApproximate(),
            originalAggCall.ignoreNulls(),
            originalAggCall.getArgList(),
            originalAggCall.filterArg,
            originalAggCall.distinctKeys,
            originalAggCall.getCollation(),
            groupCount,
            args.getInput(),
            null,
            null);

    var countAgg =
        AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            originalAggCall.getArgList(),
            originalAggCall.filterArg,
            originalAggCall.distinctKeys,
            originalAggCall.getCollation(),
            groupCount,
            args.getInput(),
            null,
            null);

    return ImmutableList.of(sumAgg, countAgg);
  }

  @Override
  protected ImmutableList<AggregateCall> doMakeGlobalAggCall(SplitterArgs args) {
    var call = originalAggCall;
    var sumAgg =
        AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            false,
            false,
            ImmutableIntList.of(prevStageIndexes.get(0)),
            call.filterArg,
            call.distinctKeys,
            call.getCollation(),
            groupCount,
            args.getInput(),
            null,
            null);

    var sum2Agg =
        AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            false,
            false,
            ImmutableIntList.of(prevStageIndexes.get(1)),
            call.filterArg,
            call.distinctKeys,
            call.getCollation(),
            groupCount,
            args.getInput(),
            null,
            null);

    return ImmutableList.of(sumAgg, sum2Agg);
  }

  @Override
  protected RexNode doMakeLastCalc(SplitterArgs args) {
    var input = args.getInput();
    var sumInputRef = rexBuilder.makeInputRef(input, prevStageIndexes.get(0));
    var sum2InputRef = rexBuilder.makeInputRef(input, prevStageIndexes.get(1));

    // TODO(xiangjin): Replace this short-term mitigation of return type mismatch between calcite
    // avg and sum/count.
    var calc = rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, sumInputRef, sum2InputRef);
    calc = rexBuilder.ensureType(originalAggCall.getType(), calc, false);
    return calc;
  }

  @Override
  public boolean isLastCalcTrivial() {
    return false;
  }
}
