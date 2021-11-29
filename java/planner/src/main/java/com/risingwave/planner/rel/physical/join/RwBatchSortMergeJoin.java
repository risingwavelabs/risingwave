package com.risingwave.planner.rel.physical.join;

import static com.google.common.base.Verify.verify;
import static com.risingwave.common.config.BatchPlannerConfigurations.ENABLE_SORT_MERGE_JOIN;
import static com.risingwave.execution.context.ExecutionContext.contextOf;
import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;
import static com.risingwave.planner.rel.physical.join.BatchJoinUtils.isEquiJoin;

import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.planner.rel.logical.RwLogicalJoin;
import com.risingwave.planner.rel.physical.RisingWaveBatchPhyRel;
import com.risingwave.proto.plan.PlanNode;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Batch sort merge join plan node. */
public class RwBatchSortMergeJoin extends RwBufferJoinBase implements RisingWaveBatchPhyRel {
  public RwBatchSortMergeJoin(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelNode left,
      RelNode right,
      RexNode condition,
      JoinRelType joinType) {
    super(cluster, traitSet, hints, left, right, condition, Collections.emptySet(), joinType);
    checkConvention();
    verify(isEquiJoin(analyzeCondition()), "Sort merge join only support equi join!");
  }

  @Override
  public PlanNode serialize() {
    throw new PgException(
        PgErrorCode.FEATURE_NOT_SUPPORTED,
        "Sort merge join executor do not support serialize to json now!");
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw);
  }

  @Override
  public Join copy(
      RelTraitSet traitSet,
      RexNode conditionExpr,
      RelNode left,
      RelNode right,
      JoinRelType joinType,
      boolean semiJoinDone) {
    return new RwBatchSortMergeJoin(
        getCluster(), traitSet, getHints(), left, right, conditionExpr, joinType);
  }

  /** Sort merge join converter rule between logical and physical. */
  public static class BatchSortMergeJoinConverterRule extends ConverterRule {
    public static final RwBatchSortMergeJoin.BatchSortMergeJoinConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(LOGICAL)
            .withOutTrait(BATCH_PHYSICAL)
            .withRuleFactory(RwBatchSortMergeJoin.BatchSortMergeJoinConverterRule::new)
            .withOperandSupplier(t -> t.operand(RwLogicalJoin.class).anyInputs())
            .withDescription("Converting logical join to batch sort merge join.")
            .as(Config.class)
            .toRule(RwBatchSortMergeJoin.BatchSortMergeJoinConverterRule.class);

    protected BatchSortMergeJoinConverterRule(Config config) {
      super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      var join = (RwLogicalJoin) call.rel(0);
      var joinInfo = join.analyzeCondition();
      return isEquiJoin(joinInfo)
          && contextOf(call).getSessionConfiguration().get(ENABLE_SORT_MERGE_JOIN);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      var join = (RwLogicalJoin) rel;
      var joinInfo = join.analyzeCondition();
      // Add collation to both left input and right input.
      var left = join.getLeft();
      var leftTraits = left.getTraitSet().plus(BATCH_PHYSICAL);
      var leftCollation = RelCollations.of(ImmutableIntList.copyOf(joinInfo.leftKeys));
      // Only add collation if the traits do not have it.
      if (!leftTraits.contains(leftCollation)) {
        leftTraits = leftTraits.plus(leftCollation);
      }
      leftTraits = leftTraits.simplify();
      var newLeft = convert(left, leftTraits);

      var right = join.getRight();
      var rightTraits = right.getTraitSet().plus(BATCH_PHYSICAL);
      var rightCollation = RelCollations.of(ImmutableIntList.copyOf(joinInfo.rightKeys));
      if (!rightTraits.contains(rightCollation)) {
        rightTraits = rightTraits.plus(rightCollation);
      }
      rightTraits = rightTraits.simplify();
      var newRight = convert(right, rightTraits);

      var newJoinTraits = newLeft.getTraitSet();

      return new RwBatchSortMergeJoin(
          join.getCluster(),
          newJoinTraits,
          join.getHints(),
          newLeft,
          newRight,
          join.getCondition(),
          join.getJoinType());
    }
  }
}
