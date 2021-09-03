package com.risingwave.planner.rel.logical;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RwLogicalJoin extends Join implements RisingWaveLogicalRel {
  public RwLogicalJoin(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelNode left,
      RelNode right,
      RexNode condition,
      JoinRelType joinType) {
    super(cluster, traitSet, hints, left, right, condition, Collections.emptySet(), joinType);
    checkConvention();
  }

  @Override
  public Join copy(
      RelTraitSet traitSet,
      RexNode conditionExpr,
      RelNode left,
      RelNode right,
      JoinRelType joinType,
      boolean semiJoinDone) {
    return new RwLogicalJoin(
        getCluster(), traitSet, getHints(), left, right, conditionExpr, joinType);
  }

  public static RwLogicalJoin create(
      RelNode left,
      RelNode right,
      List<RelHint> hints,
      RexNode condition,
      Set<CorrelationId> variablesSet,
      JoinRelType joinType,
      boolean semiJoinDon) {
    return new RwLogicalJoin(
        left.getCluster(),
        left.getTraitSet().plus(LOGICAL),
        hints,
        left,
        right,
        condition,
        joinType);
  }

  public static class RwLogicalJoinConverterRule extends ConverterRule {
    public static final RwLogicalJoinConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(Convention.NONE)
            .withOutTrait(LOGICAL)
            .withRuleFactory(RwLogicalJoinConverterRule::new)
            .withOperandSupplier(t -> t.operand(LogicalJoin.class).anyInputs())
            .withDescription("Converting logical join to risingwave version.")
            .as(Config.class)
            .toRule(RwLogicalJoinConverterRule.class);

    protected RwLogicalJoinConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      var join = (LogicalJoin) rel;

      if (!join.getVariablesSet().isEmpty()) {
        return null;
      }

      var left = RelOptRule.convert(join.getLeft(), join.getLeft().getTraitSet().plus(LOGICAL));
      var right = RelOptRule.convert(join.getRight(), join.getRight().getTraitSet().plus(LOGICAL));

      return new RwLogicalJoin(
          join.getCluster(),
          join.getTraitSet().plus(LOGICAL),
          join.getHints(),
          left,
          right,
          join.getCondition(),
          join.getJoinType());
    }
  }
}
