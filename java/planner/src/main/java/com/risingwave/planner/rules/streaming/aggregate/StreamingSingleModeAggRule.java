package com.risingwave.planner.rules.streaming.aggregate;

import static com.risingwave.execution.context.ExecutionContext.contextOf;
import static com.risingwave.planner.planner.PlannerUtils.isSingleMode;
import static com.risingwave.planner.rel.streaming.RisingWaveStreamingRel.STREAMING;
import static com.risingwave.planner.rules.streaming.aggregate.StreamingAggRuleUtility.addCountOrSumIfNotExist;
import static com.risingwave.planner.rules.streaming.aggregate.StreamingAggRuleUtility.createCountStarAggCall;
import static com.risingwave.planner.rules.streaming.aggregate.StreamingAggRuleUtility.createFilterAfterAggregate;
import static com.risingwave.planner.rules.streaming.aggregate.StreamingAggRuleUtility.createProjectAfterAggregate;

import com.risingwave.planner.rel.logical.RwLogicalAggregate;
import com.risingwave.planner.rel.streaming.RwStreamAgg;
import com.risingwave.planner.rel.streaming.RwStreamProject;
import java.util.ArrayList;
import java.util.HashSet;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * The rule for converting logical aggregation into single mode aggregation. No Exchange is needed
 * and added
 */
public class StreamingSingleModeAggRule extends RelRule<StreamingSingleModeAggRule.Config> {

  public StreamingSingleModeAggRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return isSingleMode(contextOf(call));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RwLogicalAggregate logicalAgg = call.rel(0);
    boolean simpleAgg = logicalAgg.isSimpleAgg();

    if (simpleAgg) {
      // case 1
      toSimpleAndSinglePlan(logicalAgg, call);
    } else {
      // case 3
      toNonSimpleAndSinglePlan(logicalAgg, call);
    }
  }

  private void toSimpleAndSinglePlan(RwLogicalAggregate logicalAgg, RelOptRuleCall call) {
    assert logicalAgg.getGroupCount() == 0 && logicalAgg.isSimpleAgg();
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
            createCountStarAggCall(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT)));

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
            logicalAgg, streamAgg, newAggCalls, originalCountStarIndices, countStarRefIdx, false);

    call.transformTo(project);
  }

  private void toNonSimpleAndSinglePlan(RwLogicalAggregate logicalAgg, RelOptRuleCall call) {
    assert logicalAgg.getGroupCount() != 0 && !logicalAgg.isSimpleAgg();
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
            createCountStarAggCall(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT)));

    var streamAgg =
        new RwStreamAgg(
            logicalAgg.getCluster(),
            aggTraits,
            logicalAgg.getHints(),
            newInput,
            logicalAgg.getGroupSet(),
            logicalAgg.getGroupSets(),
            newAggCalls);

    var filter = createFilterAfterAggregate(logicalAgg, streamAgg, countStarRefIdx);

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

  /** Default config */
  public interface Config extends RelRule.Config {
    StreamingSingleModeAggRule.Config DEFAULT =
        RelRule.Config.EMPTY
            .withDescription("Converting logical agg to single mode aggregation")
            .withOperandSupplier(s -> s.operand(RwLogicalAggregate.class).anyInputs())
            .as(StreamingSingleModeAggRule.Config.class);

    @Override
    default RelOptRule toRule() {
      return new StreamingSingleModeAggRule(this);
    }
  }
}
