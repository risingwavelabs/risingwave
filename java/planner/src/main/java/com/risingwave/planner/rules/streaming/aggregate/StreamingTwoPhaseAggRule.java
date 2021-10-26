package com.risingwave.planner.rules.streaming.aggregate;

import static com.risingwave.execution.context.ExecutionContext.contextOf;
import static com.risingwave.planner.planner.PlannerUtils.isDistributedMode;
import static com.risingwave.planner.rel.physical.streaming.RisingWaveStreamingRel.STREAMING;
import static com.risingwave.planner.rules.physical.batch.aggregate.AggRules.getAggSplitters;
import static com.risingwave.planner.rules.physical.batch.aggregate.AggRules.getGlobalAggCalls;
import static com.risingwave.planner.rules.physical.batch.aggregate.AggRules.getLocalAggCalls;
import static com.risingwave.planner.rules.streaming.aggregate.StreamingAggRuleUtility.addCountOrSumIfNotExist;
import static com.risingwave.planner.rules.streaming.aggregate.StreamingAggRuleUtility.createCountStarAggCall;
import static com.risingwave.planner.rules.streaming.aggregate.StreamingAggRuleUtility.createFilterAfterAggregate;
import static com.risingwave.planner.rules.streaming.aggregate.StreamingAggRuleUtility.createProjectAfterAggregate;
import static com.risingwave.planner.rules.streaming.aggregate.StreamingAggRuleUtility.createProjectForNonTrivialAggregate;
import static com.risingwave.planner.rules.streaming.aggregate.StreamingAggRuleUtility.createSum0AggCall;

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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

/** Rule for converting logical aggregation to single or distributed stream aggregation */
public class StreamingTwoPhaseAggRule extends RelRule<StreamingTwoPhaseAggRule.Config> {

  private StreamingTwoPhaseAggRule(Config config) {
    super(config);
  }

  // In the future, we expect all the rules to generate all the
  // different plans and compare the plan cost by volcano planner
  // For now, we restrict that the shuffle exchange will only work when
  // the aggregation group count must be <= 1. And of course, the cluster
  // must be in distributed mode.
  @Override
  public boolean matches(RelOptRuleCall call) {
    RwLogicalAggregate logicalAgg = call.rel(0);
    var groupCount = logicalAgg.getGroupCount();
    boolean distributedMode = isDistributedMode(contextOf(call));
    if (groupCount <= 1 && distributedMode) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RwLogicalAggregate logicalAgg = call.rel(0);
    assert isDistributedMode(contextOf(call));
    toTwoPhasePlan(logicalAgg, call);
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
            createCountStarAggCall(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT)));
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
            createSum0AggCall(countStarRefIdx + groupCount, countStarOutputType));

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
      var filter = createFilterAfterAggregate(logicalAgg, newGlobalStreamAgg, sumStarRefIdx);
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

  /** Default config */
  public interface Config extends RelRule.Config {
    StreamingTwoPhaseAggRule.Config DEFAULT =
        RelRule.Config.EMPTY
            .withDescription("Converting logical agg to two phase aggregation")
            .withOperandSupplier(s -> s.operand(RwLogicalAggregate.class).anyInputs())
            .as(StreamingTwoPhaseAggRule.Config.class);

    @Override
    default RelOptRule toRule() {
      return new StreamingTwoPhaseAggRule(this);
    }
  }
}
