package com.risingwave.planner.rules.physical.batch.aggregate;

import static com.risingwave.common.config.BatchPlannerConfigurations.ENABLE_HASH_AGG;
import static com.risingwave.execution.context.ExecutionContext.contextOf;
import static com.risingwave.planner.planner.PlannerUtils.isSingleMode;
import static com.risingwave.planner.rel.physical.batch.RisingWaveBatchPhyRel.BATCH_PHYSICAL;
import static com.risingwave.planner.rules.physical.batch.aggregate.AggRules.getAggSplitters;
import static com.risingwave.planner.rules.physical.batch.aggregate.AggRules.getGlobalAggCalls;
import static com.risingwave.planner.rules.physical.batch.aggregate.AggRules.getLocalAggCalls;
import static java.util.Collections.emptyList;

import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.logical.RwLogicalAggregate;
import com.risingwave.planner.rel.physical.batch.RwBatchHashAgg;
import com.risingwave.planner.rel.physical.batch.RwBatchProject;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rex.RexNode;

public class BatchHashAggRule extends RelRule<BatchHashAggRule.Config> {
  private BatchHashAggRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    var context = contextOf(call);
    var isSingleMode = isSingleMode(context);

    if (!isSingleMode) {
      return context.getConf().get(ENABLE_HASH_AGG);
    } else {
      return true;
    }
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RwLogicalAggregate logicalAgg = call.rel(0);

    if (logicalAgg.isSimpleAgg()) {
      // We should use stream agg for simple agg
      return;
    }

    if (isSingleMode(contextOf(call))) {
      toSingleModePlan(logicalAgg, call);
    } else {
      toDistributedPlan(logicalAgg, call);
    }
  }

  private void toSingleModePlan(RwLogicalAggregate logicalAgg, RelOptRuleCall call) {
    var requiredInputTraitSet = logicalAgg.getInput().getTraitSet().plus(BATCH_PHYSICAL);
    var aggTraits = logicalAgg.getTraitSet().plus(BATCH_PHYSICAL);
    var newInput = RelOptRule.convert(logicalAgg.getInput(), requiredInputTraitSet);
    var batchHashAgg =
        new RwBatchHashAgg(
            logicalAgg.getCluster(),
            aggTraits,
            logicalAgg.getHints(),
            newInput,
            logicalAgg.getGroupSet(),
            logicalAgg.getGroupSets(),
            logicalAgg.getAggCallList());

    call.transformTo(batchHashAgg);
  }

  private void toDistributedPlan(RwLogicalAggregate logicalAgg, RelOptRuleCall call) {
    var splitters = getAggSplitters(logicalAgg, call);

    var localAggCalls = getLocalAggCalls(logicalAgg, splitters);

    var batchAggTraits = logicalAgg.getTraitSet().plus(BATCH_PHYSICAL);
    var localBatchAggRequiredTraits = logicalAgg.getInput().getTraitSet().plus(BATCH_PHYSICAL);
    var localAggInput = RelOptRule.convert(logicalAgg.getInput(), localBatchAggRequiredTraits);

    var localBatchAgg =
        new RwBatchHashAgg(
            logicalAgg.getCluster(),
            batchAggTraits,
            logicalAgg.getHints(),
            localAggInput,
            logicalAgg.getGroupSet(),
            logicalAgg.getGroupSets(),
            localAggCalls.stream().flatMap(List::stream).collect(Collectors.toList()));

    var globalHashDist = RwDistributions.hash(logicalAgg.getGroupSet().toArray());

    var globalAggInputRequiredTraits = localBatchAgg.getTraitSet().plus(globalHashDist);

    var globalAggInput = RelOptRule.convert(localBatchAgg, globalAggInputRequiredTraits);

    var globalBatchAggCalls = getGlobalAggCalls(logicalAgg, globalAggInput, splitters);

    var newGlobalBatchAgg =
        new RwBatchHashAgg(
            logicalAgg.getCluster(),
            batchAggTraits,
            logicalAgg.getHints(),
            globalAggInput,
            logicalAgg.getGroupSet(),
            logicalAgg.getGroupSets(),
            globalBatchAggCalls.stream().flatMap(List::stream).collect(Collectors.toList()));

    var allLastCalcAreTrivial = splitters.stream().allMatch(AggSplitter::isLastCalcTrivial);

    if (allLastCalcAreTrivial) {
      call.transformTo(newGlobalBatchAgg);
    } else {
      var rexBuilder = logicalAgg.getCluster().getRexBuilder();
      var expressions = new ArrayList<RexNode>(logicalAgg.getGroupCount() + splitters.size());
      logicalAgg.getGroupSet().toList().stream()
          .map(idx -> rexBuilder.makeInputRef(newGlobalBatchAgg, idx))
          .forEachOrdered(expressions::add);

      expressions.addAll(AggRules.getLastCalcs(logicalAgg, newGlobalBatchAgg, splitters));

      var project =
          new RwBatchProject(
              logicalAgg.getCluster(),
              newGlobalBatchAgg.getTraitSet(),
              emptyList(),
              newGlobalBatchAgg,
              expressions,
              logicalAgg.getRowType());

      call.transformTo(project);
    }
  }

  public interface Config extends RelRule.Config {
    Config DEFAULT =
        RelRule.Config.EMPTY
            .withDescription("Converting logical agg to hash agg")
            .withOperandSupplier(s -> s.operand(RwLogicalAggregate.class).anyInputs())
            .as(Config.class);

    @Override
    default RelOptRule toRule() {
      return new BatchHashAggRule(this);
    }
  }
}
