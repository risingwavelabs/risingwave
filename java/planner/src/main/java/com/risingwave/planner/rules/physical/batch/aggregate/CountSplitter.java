package com.risingwave.planner.rules.physical.batch.aggregate;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableIntList;

class CountSplitter extends AbstractAggSplitter {

  CountSplitter(
      RexBuilder rexBuilder,
      int groupCount,
      AggregateCall originalAggCall,
      int originalAggCallIndex) {
    super(rexBuilder, groupCount, originalAggCall, originalAggCallIndex);
  }

  @Override
  protected ImmutableList<AggregateCall> doMakeLocalAggCall(SplitterArgs args) {
    return ImmutableList.of(originalAggCall);
  }

  @Override
  protected ImmutableList<AggregateCall> doMakeGlobalAggCall(SplitterArgs args) {
    var call = originalAggCall;

    return ImmutableList.of(
        AggregateCall.create(
            SqlStdOperatorTable.SUM,
            call.isDistinct(),
            call.isApproximate(),
            call.ignoreNulls(),
            ImmutableIntList.of(prevStageIndexes.get(0)),
            call.filterArg,
            call.distinctKeys,
            call.getCollation(),
            groupCount,
            args.getInput(),
            null,
            null));
  }
}
