package com.risingwave.planner.rules.physical.batch.join;

import static com.risingwave.planner.rel.physical.batch.RisingWaveBatchPhyRel.BATCH_PHYSICAL;

import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.logical.RwLogicalJoin;
import com.risingwave.planner.rel.physical.batch.RwBatchHashJoin;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableIntList;

public class BatchHashJoinRule extends RelRule<BatchHashJoinRule.Config> {
  protected BatchHashJoinRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    RwLogicalJoin logicalJoin = call.rel(0);
    return BatchJoinRules.isEquiJoin(logicalJoin.analyzeCondition());
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RwLogicalJoin join = call.rel(0);
    var joinInfo = join.analyzeCondition();

    var left = transformInput(join.getLeft(), joinInfo.leftKeys);
    var right = transformInput(join.getRight(), joinInfo.rightKeys);

    var batchJoin =
        new RwBatchHashJoin(
            join.getCluster(),
            join.getTraitSet().plus(BATCH_PHYSICAL),
            join.getHints(),
            left,
            right,
            join.getCondition(),
            join.getJoinType());

    call.transformTo(batchJoin);
  }

  private static RelNode transformInput(RelNode input, ImmutableIntList keys) {
    var requiredTraits = input.getTraitSet().plus(BATCH_PHYSICAL).plus(RwDistributions.hash(keys));
    return RelOptRule.convert(input, requiredTraits);
  }

  public interface Config extends RelRule.Config {
    Config DEFAULT =
        Config.EMPTY
            .withDescription("RisingWaveBatchHashJoinRule")
            .withOperandSupplier(t -> t.operand(RwLogicalJoin.class).anyInputs())
            .as(Config.class);

    @Override
    default RelOptRule toRule() {
      return new BatchHashJoinRule(this);
    }
  }
}
