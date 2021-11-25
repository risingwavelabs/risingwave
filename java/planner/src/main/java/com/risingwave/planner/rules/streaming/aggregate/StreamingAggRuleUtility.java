package com.risingwave.planner.rules.streaming.aggregate;

import static java.util.Collections.emptyList;

import com.risingwave.planner.rel.logical.RwLogicalAggregate;
import com.risingwave.planner.rel.physical.streaming.RwStreamAgg;
import com.risingwave.planner.rel.physical.streaming.RwStreamFilter;
import com.risingwave.planner.rel.physical.streaming.RwStreamProject;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

/** Utility functions used in aggregation related rules */
public final class StreamingAggRuleUtility {
  //  We differentiate 6 cases:
  //  1. simple and single.
  //   a. Add COUNT if necessary, and STREAM_NULL_BY_ROW_COUNT.
  //   b. No exchange.
  //  2. simple and 2-phase agg.
  //   a. Add COUNT on local agg and SUM on global agg if necessary, and STREAM_NULL_BY_ROW_COUNT.
  //   b. Exchange by singleton distribution.
  //  3. non-simple and single.
  //   a. Add COUNT if necessary, and add FILTER to discard results when COUNT is 0.
  //   b. Exchange by hash distribution of the group by key
  //  4. non-simple and 2-phase agg.
  //   a. Add COUNT on local agg and SUM on global agg if necessary, and add Filter to discard
  //  results when SUM is 0.
  //   b. Exchange by hash distribution of the group by key
  //  5. simple and shuffle agg.
  //   a. We don't have this combination for now as we assume simple is much better to be done in
  // 2-phase agg.
  //  6. non-simple and shuffle agg.
  //   a. Add COUNT on agg(only one) if necessary, and STREAM_NUL_BY_ROW_COUNT.
  //   b. Exchange by hash distribution of the group by key

  // The reason we need `stream_null_by_row_count` function is that
  // in streaming workload, there are appends(updates) and retractions(deletions).
  // So when we decide the output of some aggregation function, we need to consider
  // two cases:
  // 1. output 0 because the aggregation function has non-zero rows of inputs and computes to be 0
  // 2. output NULL because the aggregation function has zero rows of inputs.
  public static final SqlFunction STREAM_NULL_BY_ROW_COUNT =
      new SqlFunction(
          "$STREAM_NULL_BY_ROW_COUNT",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG1,
          null,
          null,
          SqlFunctionCategory.USER_DEFINED_SPECIFIC_FUNCTION);

  /**
   * @param originalAggCalls original aggregation calls in logical aggregation, excluding those we
   *     added by ourselves
   * @param newAggCalls new aggregation calls as the return value should be used for creating new
   *     aggregation operator
   * @param originalIndices original indices that contain the COUNT(*) or SUM0 we are looking for
   * @param aggCallMayAdd if original aggregation calls do NOT contain the COUNT(*) or SUM0 we are
   *     looking for, we add it by ourselves
   * @return the COUNT(*) or SUM0 index we will refer to in STREAM_NULL_BY_ROW_COUNT or Filter
   */
  public static int addCountOrSumIfNotExist(
      final List<AggregateCall> originalAggCalls,
      final List<AggregateCall> newAggCalls,
      final Set<Integer> originalIndices,
      final AggregateCall aggCallMayAdd) {
    // find the indices of all the aggregation calls that are equal to Count(*)
    newAggCalls.addAll(originalAggCalls);
    for (int idx = 0; idx < newAggCalls.size(); idx++) {
      var aggCall = newAggCalls.get(idx);
      // we find a COUNT(*) which can serve as ROW COUNT or a SUM0(index of count(*)) which can
      // serve
      // as ROW SUM0.
      if (aggCall.equals(aggCallMayAdd)) {
        originalIndices.add(idx);
      }
    }

    // we need a COUNT(*)/SUM0(*) as reference in each STREAM_NULL_BY_ROW_COUNT function
    var refIdx = -1;
    // if we don't have a COUNT(*)/SUM0(*), then we must add one for stream_null_by_row_count to use
    if (originalIndices.isEmpty()) {
      // the newly added count(*) is at the last position
      refIdx = newAggCalls.size();
      newAggCalls.add(aggCallMayAdd);
    } else {
      refIdx = originalIndices.iterator().next();
    }

    return refIdx;
  }

  /**
   * @param logicalAgg the original logical aggregation
   * @param input the input for the new Project operator
   * @param calculateExpressions calculate expressions used for non trivial aggregation function
   *     such as AVG(...)
   * @return the new Project operator
   */
  public static RwStreamProject createProjectForNonTrivialAggregate(
      final RwLogicalAggregate logicalAgg,
      final RelNode input,
      final List<RexNode> calculateExpressions) {
    var expressions = new ArrayList<RexNode>();
    var rexBuilder = logicalAgg.getCluster().getRexBuilder();

    // We first need to project those group by keys
    logicalAgg.getGroupSet().toList().stream()
        .map(idx -> rexBuilder.makeInputRef(input, idx))
        .forEachOrdered(expressions::add);

    // The extra things we need to take care of when there is agg that would split
    // For example, AVG(C1) would be split into SUM(C1) and COUNT(C1).
    // We remark that, after the STREAM_NULL_BY_ROW_COUNT, SUM and COUNT can be NULL,
    // but it is expected as AVG=SUM/COUNT would also output NULL when SUM or/and COUNT are NULL.
    expressions.addAll(calculateExpressions);
    var project =
        new RwStreamProject(
            logicalAgg.getCluster(),
            input.getTraitSet(),
            emptyList(),
            input,
            expressions,
            // We remark that the final output should be the same as
            // the output of the original row type
            logicalAgg.getRowType());
    return project;
  }

  /**
   * @param logicalAgg the original logical aggregation
   * @param streamAgg the input for the new Filter operator
   * @param newlyAddedIndex the index of the COUNT(*) or SUM0 we added for the aggregation
   * @return the new Filter operator
   */
  public static RwStreamFilter createFilterAfterAggregate(
      final RwLogicalAggregate logicalAgg, final RwStreamAgg streamAgg, final int newlyAddedIndex) {
    RexBuilder rexBuilder = logicalAgg.getCluster().getRexBuilder();
    var groupCount = logicalAgg.getGroupCount();
    RexNode condition =
        rexBuilder.makeCall(
            SqlStdOperatorTable.NOT_EQUALS,
            rexBuilder.makeInputRef(streamAgg, newlyAddedIndex + groupCount),
            rexBuilder.makeZeroLiteral(
                rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT)));

    var filter =
        new RwStreamFilter(logicalAgg.getCluster(), streamAgg.getTraitSet(), streamAgg, condition);
    return filter;
  }

  /**
   * @param logicalAgg the original logical aggregation
   * @param streamAgg the input for the new Project operator
   * @param newAggCalls the new aggregation calls we used for streamAgg
   * @param originalIndices original indices that contain the COUNT(*) or SUM0 we are looking for
   * @param newlyAddedIndex the index of the COUNT(*) or SUM0 we added for the aggregation
   * @param isAfterLocalAgg if it is after local agg, it needs to keep the newly add COUNT.
   *     Otherwise, remove it.
   * @return the new Project operator
   */
  public static RwStreamProject createProjectAfterAggregate(
      final RwLogicalAggregate logicalAgg,
      final RwStreamAgg streamAgg,
      final List<AggregateCall> newAggCalls,
      final Set<Integer> originalIndices,
      final int newlyAddedIndex,
      final boolean isAfterLocalAgg) {
    // We abuse the name of newlyAddedIndex here
    // as it may be
    // 1. an index of the original count(*)/sum0 contained in originalIndices or
    // 2. an index of a newly added one
    // But this produces the behavior we expect in the following loop as case 1 would be shadowed in
    // originalIndices.

    RexBuilder rexBuilder = logicalAgg.getCluster().getRexBuilder();
    var expressions = new ArrayList<RexNode>();
    // There are two types of columns, group set columns and aggregate columns
    // Here we first add those group set columns
    logicalAgg.getGroupSet().toList().stream()
        .map(idx -> rexBuilder.makeInputRef(streamAgg, idx))
        .forEachOrdered(expressions::add);
    var groupCount = logicalAgg.getGroupCount();

    // For aggregate columns, we have three cases:
    // 1. for those non-count(*)/sum0, we just wrap them within the STREAM_NULL_BY_ROW_COUNT
    // function and output
    // 2. for those count(*)/sum0 that are in the original aggregate, we keep them and do nothing
    // and output
    // 3. for the optional count(*)/sum0 that we may add additionally:
    //    a. if it is after the local aggregation, we DO output
    //    b. if it is after the global/single aggregation, we do NOT output.
    // Another assumption here is that all the group by keys are always before agg calls.
    for (int idx = 0; idx < newAggCalls.size(); idx++) {
      if (!originalIndices.contains(idx) && idx != newlyAddedIndex) {
        // (1)
        RexNode function =
            rexBuilder.makeCall(
                STREAM_NULL_BY_ROW_COUNT,
                rexBuilder.makeInputRef(streamAgg, newlyAddedIndex + groupCount),
                rexBuilder.makeInputRef(streamAgg, idx + groupCount));
        expressions.add(function);
      } else if (originalIndices.contains(idx)) {
        // (2)
        RexNode function = rexBuilder.makeInputRef(streamAgg, idx + groupCount);
        expressions.add(function);
      } else if (isAfterLocalAgg && idx == newlyAddedIndex) {
        RexNode function = rexBuilder.makeInputRef(streamAgg, idx + groupCount);
        expressions.add(function);
      }
    }

    // Get the output type after agg call is split, if we are adding project after a local
    // aggregator for 2-phase aggregation.
    var rowType = logicalAgg.getRowType();
    assert rowType.isStruct();
    // if it is
    // 1. a project after local Agg and
    // 2. we DO need to add a COUNT
    // then we need to add a column for COUNT column
    var newRowType = rowType;
    if (isAfterLocalAgg && !originalIndices.contains(newlyAddedIndex)) {
      StructKind kind = rowType.getStructKind();
      var newTypeList = new ArrayList<RelDataType>();
      var newFieldList = new ArrayList<String>();
      rowType.getFieldList().stream()
          .map(field -> field.getType())
          .forEachOrdered(newTypeList::add);
      newTypeList.add(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT));
      rowType.getFieldList().stream()
          .map(field -> field.getName())
          .forEachOrdered(newFieldList::add);
      newFieldList.add("Row Count");
      newRowType =
          streamAgg.getCluster().getTypeFactory().createStructType(kind, newTypeList, newFieldList);
    }
    assert newRowType.isStruct();
    assert (newRowType.getFieldList().size() == expressions.size())
        : String.format("New Row Type:%s Expression:%s", newRowType.getFieldList(), expressions);

    var project =
        new RwStreamProject(
            streamAgg.getCluster(),
            streamAgg.getTraitSet(),
            emptyList(),
            streamAgg,
            expressions,
            newRowType);

    return project;
  }

  /**
   * @param inputType the input type
   * @return a COUNT(*) AggregateCall
   */
  public static AggregateCall createCountStarAggCall(RelDataType inputType) {
    List<Integer> argList = new ArrayList<>();
    String name = "Row Count";
    // COUNT(*) never return NULL.
    AggregateCall countStarCall =
        new AggregateCall(SqlStdOperatorTable.COUNT, false, argList, inputType, name);
    return countStarCall;
  }

  /**
   * @param inputRefIndex the input index
   * @param inputType the input type
   * @return a SUM0(...) AggregateCall
   */
  public static AggregateCall createSum0AggCall(int inputRefIndex, RelDataType inputType) {
    List<Integer> argList = new ArrayList<>();
    argList.add(inputRefIndex);
    String name = "Row Sum0";
    // SUM0(expr) would return 0 instead of NULL when there is no row.
    // This is used for global/non-simple agg.
    AggregateCall sum0Call =
        new AggregateCall(SqlStdOperatorTable.SUM0, false, argList, inputType, name);
    return sum0Call;
  }
}
