package com.risingwave.planner.rel.physical.join;

import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;
import static com.risingwave.planner.rel.physical.join.BatchJoinUtils.isEquiJoin;

import com.google.protobuf.Any;
import com.risingwave.planner.rel.logical.RwLogicalJoin;
import com.risingwave.planner.rel.physical.RisingWaveBatchPhyRel;
import com.risingwave.planner.rel.serialization.RexToProtoSerializer;
import com.risingwave.proto.plan.NestedLoopJoinNode;
import com.risingwave.proto.plan.PlanNode;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Batch physical plan node for nested loop join. */
public class RwBatchNestedLoopJoin extends RwBufferJoinBase implements RisingWaveBatchPhyRel {
  public RwBatchNestedLoopJoin(
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
  public PlanNode serialize() {
    RexToProtoSerializer rexVisitor = new RexToProtoSerializer();
    NestedLoopJoinNode joinNode =
        NestedLoopJoinNode.newBuilder()
            .setJoinCond(condition.accept(rexVisitor))
            .setJoinType(BatchJoinUtils.getJoinTypeProto(joinType))
            .build();
    return PlanNode.newBuilder()
        .setNodeType(PlanNode.PlanNodeType.NESTED_LOOP_JOIN)
        .setBody(Any.pack(joinNode))
        .addChildren(((RisingWaveBatchPhyRel) left).serialize())
        .addChildren(((RisingWaveBatchPhyRel) right).serialize())
        .build();
  }

  @Override
  public Join copy(
      RelTraitSet traitSet,
      RexNode conditionExpr,
      RelNode left,
      RelNode right,
      JoinRelType joinType,
      boolean semiJoinDone) {
    return new RwBatchNestedLoopJoin(
        getCluster(), traitSet, getHints(), left, right, conditionExpr, joinType);
  }

  /** Nested loop join converter rule between logical and physical. */
  public static class BatchNestedLoopJoinConverterRule extends ConverterRule {
    public static final RwBatchNestedLoopJoin.BatchNestedLoopJoinConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(LOGICAL)
            .withOutTrait(BATCH_PHYSICAL)
            .withRuleFactory(RwBatchNestedLoopJoin.BatchNestedLoopJoinConverterRule::new)
            .withOperandSupplier(t -> t.operand(RwLogicalJoin.class).anyInputs())
            .withDescription("Converting logical join to batch nested loop join.")
            .as(Config.class)
            .toRule(RwBatchNestedLoopJoin.BatchNestedLoopJoinConverterRule.class);

    protected BatchNestedLoopJoinConverterRule(Config config) {
      super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      var join = (RwLogicalJoin) call.rel(0);
      var joinInfo = join.analyzeCondition();

      return !isEquiJoin(joinInfo);
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

      return new RwBatchNestedLoopJoin(
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
