package com.risingwave.planner.rules.logical;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;

/** NormalizeJoinConditionRule pulls common factors from OR's operands in join condition. */
public class NormalizeJoinConditionRule extends RelRule<NormalizeJoinConditionRule.Config> {

  public NormalizeJoinConditionRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    LogicalJoin join = call.rel(0);
    var condition = join.getCondition();
    return condition.getKind() == SqlKind.OR;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalJoin join = call.rel(0);
    var condition = join.getCondition();
    // Pull the common factors from OR's operands.
    var newCondition = RexUtil.pullFactors(join.getCluster().getRexBuilder(), condition);
    call.transformTo(
        join.copy(
            join.getTraitSet(),
            newCondition,
            join.getLeft(),
            join.getRight(),
            join.getJoinType(),
            join.isSemiJoinDone()));
  }

  /** Default Config */
  public interface Config extends RelRule.Config {
    NormalizeJoinConditionRule.Config DEFAULT =
        NormalizeJoinConditionRule.Config.EMPTY
            .withDescription("Extract Equal Join Condition")
            .withOperandSupplier(s -> s.operand(LogicalJoin.class).anyInputs())
            .as(NormalizeJoinConditionRule.Config.class);

    default NormalizeJoinConditionRule toRule() {
      return new NormalizeJoinConditionRule(this);
    }
  }
}
