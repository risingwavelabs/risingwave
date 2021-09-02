package com.risingwave.planner.rel.physical.batch;

import com.risingwave.proto.plan.PlanNode;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;

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
  }

  @Override
  public PlanNode serialize() {
    throw new UnsupportedOperationException();
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
