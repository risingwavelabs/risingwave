package com.risingwave.planner.rel.physical.streaming.join;

import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;
import static com.risingwave.planner.rel.physical.batch.join.BatchJoinUtils.isEquiJoin;

import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.logical.RwLogicalJoin;
import com.risingwave.planner.rel.physical.batch.join.BatchJoinUtils;
import com.risingwave.planner.rel.physical.streaming.RisingWaveStreamingRel;
import com.risingwave.planner.rel.physical.streaming.RwStreamingRelVisitor;
import com.risingwave.proto.streaming.plan.HashJoinNode;
import com.risingwave.proto.streaming.plan.StreamNode;
import java.util.Collections;
import java.util.List;
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

/** Stream Hash Join */
public class RwStreamHashJoin extends Join implements RisingWaveStreamingRel {

  public RwStreamHashJoin(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelNode left,
      RelNode right,
      RexNode condition,
      JoinRelType joinType) {
    super(cluster, traitSet, hints, left, right, condition, Collections.emptySet(), joinType);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw);
  }

  @Override
  public StreamNode serialize() {
    var builder = HashJoinNode.newBuilder();

    builder.setJoinType(BatchJoinUtils.getJoinTypeProto(getJoinType()));

    var joinInfo = analyzeCondition();
    joinInfo.leftKeys.forEach(builder::addLeftKey);

    joinInfo.rightKeys.forEach(builder::addRightKey);

    var hashJoinNode = builder.build();

    return StreamNode.newBuilder().setHashJoinNode(hashJoinNode).build();
  }

  @Override
  public Join copy(
      RelTraitSet traitSet,
      RexNode conditionExpr,
      RelNode left,
      RelNode right,
      JoinRelType joinType,
      boolean semiJoinDone) {
    return new RwStreamHashJoin(
        getCluster(), traitSet, getHints(), left, right, conditionExpr, joinType);
  }

  @Override
  public <T> RwStreamingRelVisitor.Result<T> accept(RwStreamingRelVisitor<T> visitor) {
    return visitor.visit(this);
  }

  /** Rule for converting logical join to stream hash join */
  public static class StreamHashJoinConverterRule extends ConverterRule {
    public static final RwStreamHashJoin.StreamHashJoinConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(LOGICAL)
            .withOutTrait(STREAMING)
            .withRuleFactory(RwStreamHashJoin.StreamHashJoinConverterRule::new)
            .withOperandSupplier(t -> t.operand(RwLogicalJoin.class).anyInputs())
            .withDescription("Converting logical join to streaming hash join.")
            .as(Config.class)
            .toRule(RwStreamHashJoin.StreamHashJoinConverterRule.class);

    protected StreamHashJoinConverterRule(Config config) {
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
      var leftKeys = join.analyzeCondition().leftKeys;
      var leftDistTrait = RwDistributions.hash(leftKeys);
      var leftTraits = left.getTraitSet().plus(STREAMING).plus(leftDistTrait);
      leftTraits = leftTraits.simplify();

      var newLeft = convert(left, leftTraits);

      var right = join.getRight();
      var rightKeys = join.analyzeCondition().rightKeys;
      var rightDistTrait = RwDistributions.hash(rightKeys);
      var rightTraits = right.getTraitSet().plus(STREAMING).plus(rightDistTrait);
      rightTraits = rightTraits.simplify();

      var newRight = convert(right, rightTraits);

      var newJoinTraits = newLeft.getTraitSet();

      return new RwStreamHashJoin(
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
