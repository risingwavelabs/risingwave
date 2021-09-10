package com.risingwave.planner.rules.physical.batch.join;

import static com.risingwave.planner.rel.physical.batch.RisingWaveBatchPhyRel.BATCH_PHYSICAL;
import static com.risingwave.planner.rules.physical.batch.join.BatchJoinRules.isEquiJoin;

import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.logical.RwLogicalJoin;
import com.risingwave.planner.rel.physical.batch.RwBatchNestLoopJoin;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;

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
    var left = join.getLeft();
    var leftTraits = left.getTraitSet().plus(BATCH_PHYSICAL).plus(RwDistributions.ANY).simplify();
    var newLeft = convert(left, leftTraits);

    var right = join.getRight();
    var rightTraits = right.getTraitSet().plus(BATCH_PHYSICAL).plus(RwDistributions.ANY).simplify();
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
