package com.risingwave.planner.rules.physical.batch.join;

import static com.risingwave.execution.context.ExecutionContext.contextOf;
import static com.risingwave.planner.planner.PlannerUtils.isSingleMode;
import static com.risingwave.planner.rel.physical.batch.RisingWaveBatchPhyRel.BATCH_PHYSICAL;
import static com.risingwave.planner.rules.physical.batch.join.BatchJoinRules.isEquiJoin;

import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.logical.RwLogicalJoin;
import com.risingwave.planner.rel.physical.batch.RwBatchNestLoopJoin;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;

/** Implementation rule for generating nested loop join plan. */
public class BatchNestedLoopJoinRule extends RelRule<BatchNestedLoopJoinRule.Config> {
  protected BatchNestedLoopJoinRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    var join = (RwLogicalJoin) call.rel(0);
    var joinInfo = join.analyzeCondition();

    return !isEquiJoin(joinInfo);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RwLogicalJoin join = call.rel(0);
    var joinType = join.getJoinType();
    var left = join.getLeft();
    var leftTraits = left.getTraitSet().plus(BATCH_PHYSICAL);
    if (!isSingleMode(contextOf(join.getCluster()))) {
      // Full or Right Outer
      if (joinType.generatesNullsOnLeft()) {
        leftTraits = leftTraits.plus(RwDistributions.SINGLETON);
      }
    }
    leftTraits = leftTraits.simplify();
    var newLeft = convert(left, leftTraits);

    var right = join.getRight();
    var rightTraits = right.getTraitSet().plus(BATCH_PHYSICAL);
    if (!isSingleMode(contextOf(join.getCluster()))) {
      // Full or Right Outer
      if (joinType.generatesNullsOnLeft()) {
        rightTraits = rightTraits.plus(RwDistributions.SINGLETON);
      } else {
        rightTraits = rightTraits.plus(RwDistributions.BROADCAST_DISTRIBUTED);
      }
    }
    rightTraits = rightTraits.simplify();

    var newRight = convert(right, rightTraits);

    var newJoinTraits = newLeft.getTraitSet();
    var newJoin =
        new RwBatchNestLoopJoin(
            join.getCluster(),
            newJoinTraits,
            join.getHints(),
            newLeft,
            newRight,
            join.getCondition(),
            join.getJoinType());

    call.transformTo(newJoin);
  }

  /** Configuration for {@link BatchNestedLoopJoinRule}. */
  public interface Config extends RelRule.Config {
    Config DEFAULT =
        Config.EMPTY
            .withDescription("RisingWaveBatchNestedLoopJoinRule")
            .withOperandSupplier(t -> t.operand(RwLogicalJoin.class).anyInputs())
            .as(Config.class);

    @Override
    default RelOptRule toRule() {
      return new BatchNestedLoopJoinRule(this);
    }
  }
}
