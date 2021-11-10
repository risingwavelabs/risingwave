package com.risingwave.planner.rel.physical.batch;

import com.google.protobuf.Any;
import com.risingwave.proto.plan.LimitNode;
import com.risingwave.proto.plan.PlanNode;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Limit in Batch convention */
public class RwBatchLimit extends SingleRel implements RisingWaveBatchPhyRel {

  public final @Nullable RexNode offset;
  public final @Nullable RexNode fetch;

  public RwBatchLimit(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      @Nullable RexNode offset,
      @Nullable RexNode fetch) {
    super(cluster, traitSet, input);
    this.offset = offset;
    this.fetch = fetch;
  }

  @Override
  public PlanNode serialize() {
    LimitNode.Builder builder = LimitNode.newBuilder();
    if (this.fetch != null) {
      builder.setLimit(RexLiteral.intValue(this.fetch));
    }
    if (this.offset != null) {
      builder.setOffset(RexLiteral.intValue(this.offset));
    }
    LimitNode limitNode = builder.build();
    return PlanNode.newBuilder()
        .setNodeType(PlanNode.PlanNodeType.LIMIT)
        .setBody(Any.pack(limitNode))
        .build();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new RwBatchLimit(this.getCluster(), traitSet, sole(inputs), offset, fetch);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .itemIf("offset", offset, offset != null)
        .itemIf("fetch", fetch, fetch != null);
  }
}
