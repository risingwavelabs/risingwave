package com.risingwave.planner.rel.physical.batch;

import com.google.protobuf.Any;
import com.risingwave.planner.rel.serialization.RexToProtoSerializer;
import com.risingwave.planner.rules.physical.batch.join.BatchJoinRules;
import com.risingwave.proto.plan.NestedLoopJoinNode;
import com.risingwave.proto.plan.PlanNode;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;

/** Batch physical plan node nest loop join. */
public class RwBatchNestLoopJoin extends Join implements RisingWaveBatchPhyRel {
  public RwBatchNestLoopJoin(
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
            .setJoinType(BatchJoinRules.getJoinTypeProto(joinType))
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
    return new RwBatchNestLoopJoin(
        getCluster(), traitSet, getHints(), left, right, conditionExpr, joinType);
  }
}
