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
import com.risingwave.planner.rel.physical.streaming.RwStreamProject;
import com.risingwave.planner.rules.physical.batch.aggregate.AggRules;
import com.risingwave.planner.rules.physical.batch.aggregate.AggSplitter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
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

  @Override
  public void onMatch(RelOptRuleCall call) {
    RwLogicalAggregate logicalAgg = call.rel(0);
    System.out.println(
        "StreamingAggRule onMatch. logicalAgg is simple?" + logicalAgg.isSimpleAgg());

    if (isSingleMode(contextOf(call)) || logicalAgg.isSimpleAgg()) {
      toSingleModePlan(logicalAgg, call);
    } else {
      toDistributedPlan(logicalAgg, call);
    }
  }

  // The reason we need `stream_null_by_row_count` function is that
  // in streaming workload, there are appends(updates) and retractions(deletions).
  // So when we decide the output of some aggregation function, we need to consider
  // two cases:
  // 1. output 0 because the aggregation function has non-zero rows of inputs and computes to be 0
  // 2. output NULL because the aggregation function has zero rows of inputs.
  private void toSingleModePlan(RwLogicalAggregate logicalAgg, RelOptRuleCall call) {
    var requiredInputTraitSet = logicalAgg.getInput().getTraitSet().plus(STREAMING);
    var aggTraits = logicalAgg.getTraitSet().plus(STREAMING);
    var newInput = RelOptRule.convert(logicalAgg.getInput(), requiredInputTraitSet);
    var rexBuilder = logicalAgg.getCluster().getRexBuilder();

    // find the indices of all the aggregation calls that are equal to Count(*)
    var countStarIndices = new HashSet<Integer>();
    var aggCalls = new ArrayList<AggregateCall>();
    aggCalls.addAll(logicalAgg.getAggCallList());
    for (int idx = 0; idx < aggCalls.size(); idx++) {
      var aggCall = aggCalls.get(idx);
      // we find a COUNT(*) which can serve as ROW COUNT;
      // TODO: here we assume that calcite would optimize all count(constant) to count(*)
      if (aggCall.getAggregation().getKind() == SqlKind.COUNT
          && !aggCall.isDistinct()
          && aggCall.getArgList().size() == 0) {
        countStarIndices.add(idx);
      }
    }

    // we need a count(*) as reference in each STREAM_NULL_BY_ROW_COUNT function
    var countStarRefIdx = -1;
    // if we don't have a Count(*), then we must add one for stream_null_by_row_count to use
    if (countStarIndices.isEmpty()) {
      RelDataType type = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
      List<Integer> argList = new ArrayList<>();
      String name = "Row Count";
      AggregateCall countStarCall =
          new AggregateCall(SqlStdOperatorTable.COUNT, false, argList, type, name);
      // the newly added count(*) is at the last position
      countStarRefIdx = aggCalls.size();
      aggCalls.add(countStarCall);
    } else {
      countStarRefIdx = countStarIndices.iterator().next();
    }

    var streamAgg =
        new RwStreamAgg(
            logicalAgg.getCluster(),
            aggTraits,
            logicalAgg.getHints(),
            newInput,
            logicalAgg.getGroupSet(),
            logicalAgg.getGroupSets(),
            aggCalls);

    // We add a project that filters out the result of all the other aggregate functions except
    // Count(*)
    // Count(*) always has a non-null output.
    var expressions = new ArrayList<RexNode>(logicalAgg.getGroupCount());
    // There are two types of columns, group set columns and aggregate columns
    // Here we first add those group set columns
    logicalAgg.getGroupSet().toList().stream()
        .map(idx -> rexBuilder.makeInputRef(streamAgg, idx))
        .forEachOrdered(expressions::add);
    var groupCount = logicalAgg.getGroupCount();

    // For aggregate columns, we have three cases:
    // 1. for those non-count(*), we just wrap them within the STREAM_NULL_BY_ROW_COUNT function
    // 2. for those count(*) that are in the original aggregate, we keep them and do nothing
    // 3. for the optional count(*) that we may add additionally, we don't keep them in project
    // Another assumption here is that all the group by keys are always before agg calls.
    for (int idx = 0; idx < aggCalls.size(); idx++) {
      if (!countStarIndices.contains(idx) && idx != countStarRefIdx) {
        // (1)
        RexNode function =
            rexBuilder.makeCall(
                STREAM_NULL_BY_ROW_COUNT,
                rexBuilder.makeInputRef(streamAgg, countStarRefIdx + groupCount),
                rexBuilder.makeInputRef(streamAgg, idx + groupCount));
        expressions.add(function);
      } else if (countStarIndices.contains(idx)) {
        // (2)
        RexNode function = rexBuilder.makeInputRef(streamAgg, idx + groupCount);
        expressions.add(function);
      }
    }

    // get the original output type
    var rowType = logicalAgg.getRowType();
    assert rowType.isStruct();
    assert rowType.getFieldList().size() == expressions.size();

    var project =
        new RwStreamProject(
            logicalAgg.getCluster(),
            streamAgg.getTraitSet(),
            emptyList(),
            streamAgg,
            expressions,
            rowType);

    call.transformTo(project);
  }

  // TODO: Add `RowCount` if it has not already existed, and also expression
  private void toDistributedPlan(RwLogicalAggregate logicalAgg, RelOptRuleCall call) {
    var splitters = getAggSplitters(logicalAgg, call);

    var localAggCalls = getLocalAggCalls(logicalAgg, splitters);

    var localAggTraits = logicalAgg.getTraitSet().plus(STREAMING);
    var localAggInputRequiredTraits = logicalAgg.getInput().getTraitSet().plus(STREAMING);
    var localAggInput = RelOptRule.convert(logicalAgg.getInput(), localAggInputRequiredTraits);

    var localStreamAgg =
        new RwStreamAgg(
            logicalAgg.getCluster(),
            localAggTraits,
            logicalAgg.getHints(),
            localAggInput,
            logicalAgg.getGroupSet(),
            logicalAgg.getGroupSets(),
            localAggCalls.stream().flatMap(List::stream).collect(Collectors.toList()));

    var globalHashDist = RwDistributions.hash(logicalAgg.getGroupSet().toArray());

    var globalAggInputRequiredTraits = localStreamAgg.getTraitSet().plus(globalHashDist);

    var globalAggInput = RelOptRule.convert(localStreamAgg, globalAggInputRequiredTraits);

    var globalStreamAggCalls = getGlobalAggCalls(logicalAgg, globalAggInput, splitters);

    var newGlobalStreamAgg =
        new RwStreamAgg(
            logicalAgg.getCluster(),
            localAggTraits,
            logicalAgg.getHints(),
            globalAggInput,
            logicalAgg.getGroupSet(),
            logicalAgg.getGroupSets(),
            globalStreamAggCalls.stream().flatMap(List::stream).collect(Collectors.toList()));

    var allLastCalcAreTrivial = splitters.stream().allMatch(AggSplitter::isLastCalcTrivial);

    if (allLastCalcAreTrivial) {
      call.transformTo(newGlobalStreamAgg);
    } else {
      var rexBuilder = logicalAgg.getCluster().getRexBuilder();
      var expressions = new ArrayList<RexNode>(logicalAgg.getGroupCount() + splitters.size());
      logicalAgg.getGroupSet().toList().stream()
          .map(idx -> rexBuilder.makeInputRef(newGlobalStreamAgg, idx))
          .forEachOrdered(expressions::add);

      expressions.addAll(AggRules.getLastCalcs(logicalAgg, newGlobalStreamAgg, splitters));

      var project =
          new RwStreamProject(
              logicalAgg.getCluster(),
              newGlobalStreamAgg.getTraitSet(),
              emptyList(),
              newGlobalStreamAgg,
              expressions,
              logicalAgg.getRowType());

      call.transformTo(project);
    }
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
