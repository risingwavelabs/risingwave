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
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rex.RexNode;

/** Rule for converting logical aggregation to single or distributed stream aggregation */
public class StreamingAggRule extends RelRule<StreamingAggRule.Config> {

  private StreamingAggRule(Config config) {
    super(config);
  }

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

  // TODO: Add `RowCount` if it has not already existed, and also expression
  // `stream_null_by_row_count` function
  private void toSingleModePlan(RwLogicalAggregate logicalAgg, RelOptRuleCall call) {
    var requiredInputTraitSet = logicalAgg.getInput().getTraitSet().plus(STREAMING);
    var aggTraits = logicalAgg.getTraitSet().plus(STREAMING);
    var newInput = RelOptRule.convert(logicalAgg.getInput(), requiredInputTraitSet);
    var streamAgg =
        new RwStreamAgg(
            logicalAgg.getCluster(),
            aggTraits,
            logicalAgg.getHints(),
            newInput,
            logicalAgg.getGroupSet(),
            logicalAgg.getGroupSets(),
            logicalAgg.getAggCallList());

    call.transformTo(streamAgg);
  }

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
