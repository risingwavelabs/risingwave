package com.risingwave.planner.rel.physical.join;

import static com.google.common.base.Verify.verify;
import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;
import static com.risingwave.planner.rel.physical.join.BatchJoinUtils.isEquiJoin;

import com.google.protobuf.Any;
import com.risingwave.planner.rel.logical.RwLogicalJoin;
import com.risingwave.planner.rel.physical.RisingWaveBatchPhyRel;
import com.risingwave.proto.plan.HashJoinNode;
import com.risingwave.proto.plan.PlanNode;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Batch hash join plan node. */
public class RwBatchHashJoin extends RwBufferJoinBase implements RisingWaveBatchPhyRel {
  public RwBatchHashJoin(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelNode left,
      RelNode right,
      RexNode condition,
      JoinRelType joinType) {
    super(cluster, traitSet, hints, left, right, condition, Collections.emptySet(), joinType);
    checkConvention();
    verify(BatchJoinUtils.isEquiJoin(analyzeCondition()), "Hash join only support equi join!");
  }

  @Override
  public PlanNode serialize() {
    var builder = HashJoinNode.newBuilder();

    builder.setJoinType(BatchJoinUtils.getJoinTypeProto(getJoinType()));

    var joinInfo = analyzeCondition();
    joinInfo.leftKeys.forEach(builder::addLeftKey);
    IntStream.range(0, left.getRowType().getFieldCount()).forEachOrdered(builder::addLeftOutput);

    joinInfo.rightKeys.forEach(builder::addRightKey);
    IntStream.range(0, right.getRowType().getFieldCount()).forEachOrdered(builder::addRightOutput);

    var hashJoinNode = builder.build();

    // TODO: Push project into join

    var leftChild = ((RisingWaveBatchPhyRel) left).serialize();
    var rightChild = ((RisingWaveBatchPhyRel) right).serialize();

    return PlanNode.newBuilder()
        .setNodeType(PlanNode.PlanNodeType.HASH_JOIN)
        .addChildren(leftChild)
        .addChildren(rightChild)
        .setBody(Any.pack(hashJoinNode))
        .build();
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
    return new RwBatchHashJoin(
        getCluster(), traitSet, getHints(), left, right, conditionExpr, joinType);
  }

  /** Hash join converter rule between logical and physical. */
  public static class BatchHashJoinConverterRule extends ConverterRule {
    public static final RwBatchHashJoin.BatchHashJoinConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(LOGICAL)
            .withOutTrait(BATCH_PHYSICAL)
            .withRuleFactory(RwBatchHashJoin.BatchHashJoinConverterRule::new)
            .withOperandSupplier(t -> t.operand(RwLogicalJoin.class).anyInputs())
            .withDescription("Converting logical join to batch hash join.")
            .as(Config.class)
            .toRule(RwBatchHashJoin.BatchHashJoinConverterRule.class);

    protected BatchHashJoinConverterRule(Config config) {
      super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      var join = (RwLogicalJoin) call.rel(0);
      var joinInfo = join.analyzeCondition();
      return isEquiJoin(joinInfo);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      var join = (RwLogicalJoin) rel;

      var left = join.getLeft();
      var leftTraits = left.getTraitSet().plus(BATCH_PHYSICAL);
      leftTraits = leftTraits.simplify();

      var newLeft = convert(left, leftTraits);

      var right = join.getRight();
      var rightTraits = right.getTraitSet().plus(BATCH_PHYSICAL);
      rightTraits = rightTraits.simplify();

      var newRight = convert(right, rightTraits);

      var newJoinTraits = newLeft.getTraitSet();

      return new RwBatchHashJoin(
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
