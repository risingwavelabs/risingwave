package com.risingwave.planner.rules.streaming.aggregate;

import static com.risingwave.execution.context.ExecutionContext.contextOf;
import static com.risingwave.planner.planner.PlannerUtils.isDistributedMode;
import static com.risingwave.planner.rel.streaming.RisingWaveStreamingRel.STREAMING;
import static com.risingwave.planner.rules.streaming.aggregate.StreamingAggRuleUtility.addCountOrSumIfNotExist;
import static com.risingwave.planner.rules.streaming.aggregate.StreamingAggRuleUtility.createCountStarAggCall;
import static com.risingwave.planner.rules.streaming.aggregate.StreamingAggRuleUtility.createFilterAfterAggregate;

import com.risingwave.planner.rel.common.dist.RwDistributions;
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

/** Rule for converting logical aggregation to shuffle aggregation */
public class StreamingShuffleAggRule extends RelRule<StreamingShuffleAggRule.Config> {

  /** Threshold of number of group keys to use shuffle aggregation */
  private static final int MIN_GROUP_COUNT = 2;

  public StreamingShuffleAggRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    RwLogicalAggregate logicalAgg = call.rel(0);
    var groupCount = logicalAgg.getGroupCount();
    return isDistributedMode(contextOf(call)) && groupCount >= MIN_GROUP_COUNT;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RwLogicalAggregate logicalAgg = call.rel(0);
    var groupCount = logicalAgg.getGroupCount();
    var originalRowType = logicalAgg.getRowType();
    assert isDistributedMode(contextOf(call)) && groupCount >= MIN_GROUP_COUNT;
    var distTrait = RwDistributions.hash(logicalAgg.getGroupSet().toArray());
    var requiredInputTraitSet = logicalAgg.getInput().getTraitSet().plus(STREAMING).plus(distTrait);
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

    // since this must not be a simple aggregation, we add Filter
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
    StreamingShuffleAggRule.Config DEFAULT =
        RelRule.Config.EMPTY
            .withDescription("Converting logical agg to shuffle aggregation")
            .withOperandSupplier(s -> s.operand(RwLogicalAggregate.class).anyInputs())
            .as(StreamingShuffleAggRule.Config.class);

    @Override
    default RelOptRule toRule() {
      return new StreamingShuffleAggRule(this);
    }
  }
}
