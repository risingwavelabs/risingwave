package com.risingwave.planner.rules.physical.batch.aggregate;

import static com.risingwave.common.config.BatchPlannerConfigurations.ENABLE_SORT_AGG;
import static com.risingwave.execution.context.ExecutionContext.contextOf;
import static com.risingwave.planner.planner.PlannerUtils.isSingleMode;
import static com.risingwave.planner.rel.physical.batch.RisingWaveBatchPhyRel.BATCH_PHYSICAL;
import static com.risingwave.planner.rules.physical.batch.aggregate.AggRules.collationOf;
import static com.risingwave.planner.rules.physical.batch.aggregate.AggRules.getAggSplitters;
import static com.risingwave.planner.rules.physical.batch.aggregate.AggRules.getGlobalAggCalls;
import static com.risingwave.planner.rules.physical.batch.aggregate.AggRules.getLocalAggCalls;
import static java.util.Collections.emptyList;

import com.risingwave.planner.rel.common.dist.RwDistributionTrait;
import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.logical.RwLogicalAggregate;
import com.risingwave.planner.rel.physical.batch.RwBatchProject;
import com.risingwave.planner.rel.physical.batch.RwBatchSortAgg;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rex.RexNode;

/** BatchSortAggRule converts a LogicalAggregate to SortAgg operator */
public class BatchSortAggRule extends RelRule<BatchSortAggRule.Config> {
  protected BatchSortAggRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    var context = contextOf(call);
    var isSingleMode = isSingleMode(context);

    if (!isSingleMode) {
      return context.getConf().get(ENABLE_SORT_AGG);
    } else {
      return true;
    }
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RwLogicalAggregate logicalAgg = call.rel(0);

    if (isSingleMode(contextOf(call))) {
      toSingleModePlan(logicalAgg, call);
    } else {
      toDistributedPlan(logicalAgg, call);
    }
  }

  private void toSingleModePlan(RwLogicalAggregate logicalAgg, RelOptRuleCall call) {
    var inputRequiredTraitSet = logicalAgg.getInput().getTraitSet().plus(BATCH_PHYSICAL);
    var aggTraits = logicalAgg.getTraitSet().plus(BATCH_PHYSICAL);

    if (logicalAgg.getGroupCount() > 0) {
      var collation = collationOf(logicalAgg.getGroupSet());
      inputRequiredTraitSet = inputRequiredTraitSet.plus(collation);
      aggTraits = aggTraits.plus(collation);
    }

    var newInput = RelOptRule.convert(logicalAgg.getInput(), inputRequiredTraitSet);
    var batchHashAgg =
        new RwBatchSortAgg(
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

    var localBatchAggInputRequiredTraits = logicalAgg.getInput().getTraitSet().plus(BATCH_PHYSICAL);
    var batchAggTraits = logicalAgg.getTraitSet().plus(BATCH_PHYSICAL);

    if (!logicalAgg.isSimpleAgg()) {
      var collation = collationOf(logicalAgg.getGroupSet());
      localBatchAggInputRequiredTraits = localBatchAggInputRequiredTraits.plus(collation);
      batchAggTraits = batchAggTraits.plus(collation);
    }

    var localBatchAggInput =
        RelOptRule.convert(logicalAgg.getInput(), localBatchAggInputRequiredTraits);

    var localBatchAgg =
        new RwBatchSortAgg(
            logicalAgg.getCluster(),
            batchAggTraits,
            logicalAgg.getHints(),
            localBatchAggInput,
            logicalAgg.getGroupSet(),
            logicalAgg.getGroupSets(),
            localAggCalls.stream().flatMap(List::stream).collect(Collectors.toList()));

    RwDistributionTrait globalDist;
    if (!logicalAgg.isSimpleAgg()) {
      globalDist = RwDistributions.hash(logicalAgg.getGroupSet().toArray());
    } else {
      globalDist = RwDistributions.SINGLETON;
    }

    var globalAggInput =
        RelOptRule.convert(localBatchAgg, localBatchAgg.getTraitSet().plus(globalDist));

    var globalAggCalls = getGlobalAggCalls(logicalAgg, globalAggInput, splitters);

    var globalBatchAgg =
        new RwBatchSortAgg(
            logicalAgg.getCluster(),
            batchAggTraits.plus(globalDist),
            logicalAgg.getHints(),
            globalAggInput,
            logicalAgg.getGroupSet(),
            logicalAgg.getGroupSets(),
            globalAggCalls.stream().flatMap(List::stream).collect(Collectors.toList()));

    var allLastCalcAreTrivial = splitters.stream().allMatch(AggSplitter::isLastCalcTrivial);

    if (allLastCalcAreTrivial) {
      call.transformTo(globalBatchAgg);
    } else {
      var rexBuilder = logicalAgg.getCluster().getRexBuilder();
      var expressions = new ArrayList<RexNode>(logicalAgg.getGroupCount() + splitters.size());
      logicalAgg.getGroupSet().toList().stream()
          .map(idx -> rexBuilder.makeInputRef(globalBatchAgg, idx))
          .forEachOrdered(expressions::add);

      expressions.addAll(AggRules.getLastCalcs(logicalAgg, globalBatchAgg, splitters));

      var project =
          new RwBatchProject(
              logicalAgg.getCluster(),
              globalBatchAgg.getTraitSet(),
              emptyList(),
              globalBatchAgg,
              expressions,
              logicalAgg.getRowType());

      call.transformTo(project);
    }
  }

  /** Rule config of BatchSortAggRule */
  public interface Config extends RelRule.Config {
    Config DEFAULT =
        RelRule.Config.EMPTY
            .withDescription("Converting agg to sort agg")
            .withOperandSupplier(s -> s.operand(RwLogicalAggregate.class).anyInputs())
            .as(Config.class);

    @Override
    default RelOptRule toRule() {
      return new BatchSortAggRule(this);
    }
  }
}
