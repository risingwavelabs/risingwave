package com.risingwave.planner.rules.streaming;

import static com.risingwave.execution.context.ExecutionContext.contextOf;
import static com.risingwave.planner.planner.PlannerUtils.isSingleMode;
import static com.risingwave.planner.rel.physical.streaming.RisingWaveStreamingRel.STREAMING;
import static com.risingwave.planner.rules.physical.batch.aggregate.AggRules.getAggSplitters;
import static com.risingwave.planner.rules.physical.batch.aggregate.AggRules.getGlobalAggCalls;
import static com.risingwave.planner.rules.physical.batch.aggregate.AggRules.getLocalAggCalls;
import static java.util.Collections.emptyList;

import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.logical.RwLogicalAggregate;
import com.risingwave.planner.rel.physical.streaming.RwStreamAgg;
import com.risingwave.planner.rel.physical.streaming.RwStreamFilter;
import com.risingwave.planner.rel.physical.streaming.RwStreamProject;
import com.risingwave.planner.rules.physical.batch.aggregate.AggRules;
import com.risingwave.planner.rules.physical.batch.aggregate.AggSplitter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
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

/** Rule for converting logical aggregation to single or distributed stream aggregation */
public class StreamingAggRule extends RelRule<StreamingAggRule.Config> {

  private StreamingAggRule(Config config) {
    super(config);
  }

  public static final SqlFunction STREAM_NULL_BY_ROW_COUNT =
      new SqlFunction(
          "$STREAM_NULL_BY_ROW_COUNT",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG1,
          null,
          null,
          SqlFunctionCategory.USER_DEFINED_SPECIFIC_FUNCTION);

  // The reason we need `stream_null_by_row_count` function is that
  // in streaming workload, there are appends(updates) and retractions(deletions).
  // So when we decide the output of some aggregation function, we need to consider
  // two cases:
  // 1. output 0 because the aggregation function has non-zero rows of inputs and computes to be 0
  // 2. output NULL because the aggregation function has zero rows of inputs.
  @Override
  public void onMatch(RelOptRuleCall call) {
    RwLogicalAggregate logicalAgg = call.rel(0);
    // We differentiate 4 cases:
    // 1. simple and single.
    //    a. Add COUNT if necessary, and STREAM_NULL_BY_ROW_COUNT.
    //    b. No exchange.
    // 2. simple and 2-phase agg.
    //    a. Add COUNT on local agg and SUM on global agg if necessary, and
    // STREAM_NULL_BY_ROW_COUNT.
    //    b. Exchange by singleton distribution.
    // 3. non-simple and single.
    //    a. Add COUNT if necessary, and add FILTER to discard results when COUNT is 0.
    //    b. Exchange by hash distribution of the group by key
    // 4. non-simple and 2-phase agg.
    //    a. Add COUNT on local agg and SUM on global agg if necessary, and add Filter to discard
    // results when SUM is 0.
    //    b. Exchange by hash distribution of the group by key
    // TODO: will add shuffle agg in another rule
    // 2-phase == local agg -> exchange -> global agg
    // shuffle == exchange -> agg

    boolean singleMode = isSingleMode(contextOf(call));
    boolean simpleAgg = logicalAgg.isSimpleAgg();

    if (singleMode) {
      if (simpleAgg) {
        // case 1
        toSimpleAndSinglePlan(logicalAgg, call);
      } else {
        // case 3
        toNonSimpleAndSinglePlan(logicalAgg, call);
      }
    } else {
      // case 2 and 4
      toTwoPhasePlan(logicalAgg, call);
    }
  }

  private void toSimpleAndSinglePlan(RwLogicalAggregate logicalAgg, RelOptRuleCall call) {
    assert logicalAgg.getGroupCount() == 0;
    var requiredInputTraitSet = logicalAgg.getInput().getTraitSet().plus(STREAMING);
    var aggTraits = logicalAgg.getTraitSet().plus(STREAMING);
    var newInput = RelOptRule.convert(logicalAgg.getInput(), requiredInputTraitSet);
    var rexBuilder = logicalAgg.getCluster().getRexBuilder();

    // find the indices of all the aggregation calls that are equal to Count(*)
    var originalCountStarIndices = new HashSet<Integer>();
    var newAggCalls = new ArrayList<AggregateCall>();
    var countStarRefIdx =
        addCountOrSumIfNotExist(
            logicalAgg.getAggCallList(),
            newAggCalls,
            originalCountStarIndices,
            createCountStarAggCall(rexBuilder));

    var streamAgg =
        new RwStreamAgg(
            logicalAgg.getCluster(),
            aggTraits,
            logicalAgg.getHints(),
            newInput,
            logicalAgg.getGroupSet(),
            logicalAgg.getGroupSets(),
            newAggCalls);

    var project =
        createProjectAfterAggregate(
            logicalAgg,
            requiredInputTraitSet,
            streamAgg,
            newAggCalls,
            originalCountStarIndices,
            countStarRefIdx,
            false);

    call.transformTo(project);
  }

  private void toNonSimpleAndSinglePlan(RwLogicalAggregate logicalAgg, RelOptRuleCall call) {
    assert logicalAgg.getGroupCount() != 0;
    var requiredInputTraitSet = logicalAgg.getInput().getTraitSet().plus(STREAMING);
    var aggTraits = logicalAgg.getTraitSet().plus(STREAMING);
    var newInput = RelOptRule.convert(logicalAgg.getInput(), requiredInputTraitSet);
    var originalRowType = logicalAgg.getRowType();
    var rexBuilder = logicalAgg.getCluster().getRexBuilder();

    // find the indices of all the aggregation calls that are equal to Count(*)
    var originalCountStarIndices = new HashSet<Integer>();
    var newAggCalls = new ArrayList<AggregateCall>();
    var countStarRefIdx =
        addCountOrSumIfNotExist(
            logicalAgg.getAggCallList(),
            newAggCalls,
            originalCountStarIndices,
            createCountStarAggCall(rexBuilder));

    var streamAgg =
        new RwStreamAgg(
            logicalAgg.getCluster(),
            aggTraits,
            logicalAgg.getHints(),
            newInput,
            logicalAgg.getGroupSet(),
            logicalAgg.getGroupSets(),
            newAggCalls);

    var filter = createFilterAfterAggregate(logicalAgg, aggTraits, streamAgg, countStarRefIdx);

    // we would need another project only if we indeed added a COUNT(*) and it is in the end.
    if (!originalCountStarIndices.contains(countStarRefIdx)) {
      var projects = new ArrayList<RexNode>();
      for (int idx = 0; idx < logicalAgg.getRowType().getFieldCount(); idx++) {
        projects.add(rexBuilder.makeInputRef(filter, idx));
      }
      var project =
          new RwStreamProject(
              logicalAgg.getCluster(),
              aggTraits,
              logicalAgg.getHints(),
              filter,
              projects,
              originalRowType);
      call.transformTo(project);
    } else {
      call.transformTo(filter);
    }
  }

  private void toTwoPhasePlan(RwLogicalAggregate logicalAgg, RelOptRuleCall call) {
    var rexBuilder = logicalAgg.getCluster().getRexBuilder();
    final var originalRowType = logicalAgg.getRowType();
    final var groupCount = logicalAgg.getGroupCount();
    var splitters = getAggSplitters(logicalAgg, call);

    var localAggCalls =
        getLocalAggCalls(logicalAgg, splitters).stream()
            .flatMap(List::stream)
            .collect(Collectors.toList());

    var localAggTraits = logicalAgg.getTraitSet().plus(STREAMING);
    var localAggInputRequiredTraits = logicalAgg.getInput().getTraitSet().plus(STREAMING);
    var localAggInput = RelOptRule.convert(logicalAgg.getInput(), localAggInputRequiredTraits);

    var originalCountStarIndices = new HashSet<Integer>();
    var newLocalAggCalls = new ArrayList<AggregateCall>();
    var countStarRefIdx =
        addCountOrSumIfNotExist(
            localAggCalls,
            newLocalAggCalls,
            originalCountStarIndices,
            createCountStarAggCall(rexBuilder));
    var countStarOutputType = newLocalAggCalls.get(countStarRefIdx).getType();

    var localStreamAgg =
        new RwStreamAgg(
            logicalAgg.getCluster(),
            localAggTraits,
            logicalAgg.getHints(),
            localAggInput,
            logicalAgg.getGroupSet(),
            logicalAgg.getGroupSets(),
            newLocalAggCalls);

    var localStreamProject =
        createProjectAfterAggregate(
            logicalAgg,
            localAggTraits,
            localStreamAgg,
            newLocalAggCalls,
            originalCountStarIndices,
            countStarRefIdx,
            true);

    // For simple aggregation we use Singleton Distribution to gather all the results into a single
    // place.
    var globalDistTrait = RwDistributions.SINGLETON;
    if (!logicalAgg.isSimpleAgg()) {
      // For non-simple aggregation, we use Hash Distribution.
      globalDistTrait = RwDistributions.hash(logicalAgg.getGroupSet().toArray());
    }

    var globalAggInputRequiredTraits = localStreamProject.getTraitSet().plus(globalDistTrait);

    var globalAggInput = RelOptRule.convert(localStreamProject, globalAggInputRequiredTraits);

    // We remark that the number of splitters is determined by the original logical aggregate,
    // so the newly added count(*) (if any) will not be considered in getGlobalAggCalls.
    // Therefore, we need to manually add a sum(*) which corresponds to the count(*) in the local
    // aggregate if we indeed added a count(*).
    // Otherwise, we should be able to find a sum(*) generated by splitter with respect to an
    // original local
    // count(*), and thus reuse this sum(*).
    var globalAggCalls =
        getGlobalAggCalls(logicalAgg, globalAggInput, splitters).stream()
            .flatMap(List::stream)
            .collect(Collectors.toList());
    // find the indices of all the aggregation calls that are equal to Count(*)
    var originalSumStarIndices = new HashSet<Integer>();
    var newGlobalAggCalls = new ArrayList<AggregateCall>();
    var sumStarRefIdx =
        addCountOrSumIfNotExist(
            globalAggCalls,
            newGlobalAggCalls,
            originalSumStarIndices,
            createSum0AggCall(rexBuilder, countStarRefIdx + groupCount, countStarOutputType));

    var newGlobalStreamAgg =
        new RwStreamAgg(
            globalAggInput.getCluster(),
            localAggTraits,
            localStreamProject.getHints(),
            globalAggInput,
            logicalAgg.getGroupSet(),
            logicalAgg.getGroupSets(),
            newGlobalAggCalls);

    if (logicalAgg.isSimpleAgg()) {
      // for case 2
      var streamNullByRowCountProject =
          createProjectAfterAggregate(
              logicalAgg,
              localAggTraits,
              newGlobalStreamAgg,
              newGlobalAggCalls,
              originalSumStarIndices,
              sumStarRefIdx,
              false);

      assert streamNullByRowCountProject.getRowType() == logicalAgg.getRowType();
      // There are two cases:
      // 1. the sum(*) is newly generated, thus we don't need to output.
      // 2. the sum(*) has already exists, thus we need to output.
      var allLastCalcAreTrivial = splitters.stream().allMatch(AggSplitter::isLastCalcTrivial);
      if (allLastCalcAreTrivial) {
        call.transformTo(streamNullByRowCountProject);
      } else {
        List<RexNode> calculateExpressions =
            AggRules.getLastCalcs(logicalAgg, streamNullByRowCountProject, splitters);
        RwStreamProject project =
            createProjectForNonTrivialAggregate(
                logicalAgg, streamNullByRowCountProject, calculateExpressions);
        call.transformTo(project);
      }
    } else {
      // for case 4
      var filter =
          createFilterAfterAggregate(logicalAgg, localAggTraits, newGlobalStreamAgg, sumStarRefIdx);
      var allLastCalcAreTrivial = splitters.stream().allMatch(AggSplitter::isLastCalcTrivial);
      boolean needToDelete = !originalSumStarIndices.contains(sumStarRefIdx);

      if (needToDelete) {
        var projects = new ArrayList<RexNode>();
        for (int idx = 0; idx < logicalAgg.getRowType().getFieldCount(); idx++) {
          projects.add(rexBuilder.makeInputRef(filter, idx));
        }
        var project =
            new RwStreamProject(
                logicalAgg.getCluster(),
                localAggTraits,
                logicalAgg.getHints(),
                filter,
                projects,
                originalRowType);

        if (allLastCalcAreTrivial) {
          call.transformTo(project);
        } else {
          List<RexNode> calculateExpressions = AggRules.getLastCalcs(logicalAgg, filter, splitters);
          RwStreamProject projectForNonTrivialAggregate =
              createProjectForNonTrivialAggregate(logicalAgg, project, calculateExpressions);
          call.transformTo(projectForNonTrivialAggregate);
        }
      } else {
        if (allLastCalcAreTrivial) {
          call.transformTo(filter);
        } else {
          List<RexNode> calculateExpressions = AggRules.getLastCalcs(logicalAgg, filter, splitters);
          RwStreamProject projectForNonTrivialAggregate =
              createProjectForNonTrivialAggregate(logicalAgg, filter, calculateExpressions);
          call.transformTo(projectForNonTrivialAggregate);
        }
      }
    }
  }

  private int addCountOrSumIfNotExist(
      final List<AggregateCall> originalAggCalls,
      List<AggregateCall> newAggCalls,
      Set<Integer> originalIndices,
      final AggregateCall aggCallMayAdd) {
    // find the indices of all the aggregation calls that are equal to Count(*)
    newAggCalls.addAll(originalAggCalls);
    for (int idx = 0; idx < newAggCalls.size(); idx++) {
      var aggCall = newAggCalls.get(idx);
      // we find a COUNT(*) which can serve as ROW COUNT;
      // TODO: here we assume that calcite would optimize all count(constant) to count(*)
      if (aggCall.getAggregation().getKind() == aggCallMayAdd.getAggregation().getKind()
          && !aggCall.isDistinct()
          && aggCall.getArgList().size() == 0) {
        originalIndices.add(idx);
      }
    }

    // we need a COUNT(*)/SUM(*) as reference in each STREAM_NULL_BY_ROW_COUNT function
    var refIdx = -1;
    // if we don't have a COUNT(*)/SUM(*), then we must add one for stream_null_by_row_count to use
    if (originalIndices.isEmpty()) {
      // the newly added count(*) is at the last position
      refIdx = newAggCalls.size();
      newAggCalls.add(aggCallMayAdd);
    } else {
      refIdx = originalIndices.iterator().next();
    }

    return refIdx;
  }

  private RwStreamProject createProjectForNonTrivialAggregate(
      RwLogicalAggregate logicalAgg, RelNode input, List<RexNode> calculateExpressions) {
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

  private RwStreamFilter createFilterAfterAggregate(
      final RwLogicalAggregate logicalAgg,
      final RelTraitSet traitsOnFilter,
      RwStreamAgg streamAgg,
      final int newlyAddedIndex) {
    RexBuilder rexBuilder = logicalAgg.getCluster().getRexBuilder();
    var groupCount = logicalAgg.getGroupCount();
    RexNode condition =
        rexBuilder.makeCall(
            SqlStdOperatorTable.NOT_EQUALS,
            rexBuilder.makeInputRef(streamAgg, newlyAddedIndex + groupCount),
            rexBuilder.makeZeroLiteral(
                rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT)));

    var filter = new RwStreamFilter(logicalAgg.getCluster(), traitsOnFilter, streamAgg, condition);
    return filter;
  }

  // Need logicalAgg just to get group count and utility related stuff.
  // Do not rely on agg calls related stuff of logicalAgg.
  private RwStreamProject createProjectAfterAggregate(
      final RwLogicalAggregate logicalAgg,
      final RelTraitSet traitsOnProject,
      RwStreamAgg streamAgg,
      List<AggregateCall> newAggCalls,
      Set<Integer> originalIndices,
      final int newlyAddedIndex,
      final boolean isAfterLocalAgg) {
    // we abuse the name of newlyAddedIndex here
    // as it may be
    // 1. an index of the original count(*)/sum(*) contained in originalIndices or
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
    // 1. for those non-count(*)/sum(*), we just wrap them within the STREAM_NULL_BY_ROW_COUNT
    // function and output
    // 2. for those count(*)/sum(*) that are in the original aggregate, we keep them and do nothing
    // and output
    // 3. for the optional count(*)/sum(*) that we may add additionally:
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

    // get the original output type
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
          logicalAgg
              .getCluster()
              .getTypeFactory()
              .createStructType(kind, newTypeList, newFieldList);
    }
    assert newRowType.isStruct();
    assert (newRowType.getFieldList().size() == expressions.size())
        : String.format("New Row Type:%s Expression:%s", newRowType.getFieldList(), expressions);

    var project =
        new RwStreamProject(
            streamAgg.getCluster(),
            traitsOnProject,
            emptyList(),
            streamAgg,
            expressions,
            newRowType);

    return project;
  }

  // COUNT(*) never return NULL.
  private AggregateCall createCountStarAggCall(RexBuilder rexBuilder) {
    RelDataType type = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
    List<Integer> argList = new ArrayList<>();
    String name = "Row Count";
    AggregateCall countStarCall =
        new AggregateCall(SqlStdOperatorTable.COUNT, false, argList, type, name);
    return countStarCall;
  }

  // SUM0(expr) would return 0 instead of NULL when there is no row.
  // This is used for global/non-simple agg.
  private AggregateCall createSum0AggCall(
      RexBuilder rexBuilder, int inputRefIndex, RelDataType inputType) {
    List<Integer> argList = new ArrayList<>();
    argList.add(inputRefIndex);
    String name = "Row Sum0";
    AggregateCall sum0Call =
        new AggregateCall(SqlStdOperatorTable.SUM0, false, argList, inputType, name);
    return sum0Call;
  }

  /** Default config */
  public interface Config extends RelRule.Config {
    StreamingAggRule.Config DEFAULT =
        RelRule.Config.EMPTY
            .withDescription("Converting logical agg to single or distributed stream agg")
            .withOperandSupplier(s -> s.operand(RwLogicalAggregate.class).anyInputs())
            .as(StreamingAggRule.Config.class);

    @Override
    default RelOptRule toRule() {
      return new StreamingAggRule(this);
    }
  }
}
