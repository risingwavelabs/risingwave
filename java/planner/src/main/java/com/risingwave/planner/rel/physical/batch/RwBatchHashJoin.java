package com.risingwave.planner.rel.physical.batch;

import static com.google.common.base.Verify.verify;

import com.google.protobuf.Any;
import com.risingwave.planner.rules.physical.batch.join.BatchJoinRules;
import com.risingwave.proto.plan.HashJoinNode;
import com.risingwave.proto.plan.PlanNode;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;

/** Batch hash join plan node. */
public class RwBatchHashJoin extends Join implements RisingWaveBatchPhyRel {
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
    verify(BatchJoinRules.isEquiJoin(analyzeCondition()), "Hash join only support equi join!");
  }

  @Override
  public PlanNode serialize() {
    var builder = HashJoinNode.newBuilder();

    builder.setJoinType(BatchJoinRules.getJoinTypeProto(getJoinType()));

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
        getCluster(), traitSet, getHints(), left, right, condition, joinType);
  }
}
