package com.risingwave.planner.rel.physical.streaming;

import com.risingwave.proto.streaming.plan.StreamNode;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

/**
 * We need to explicitly specify a materialized view node in a streaming plan.
 *
 * <p>A sequential streaming plan (no parallel degree) roots with a materialized view node.
 */
public class RwStreamMaterializedView extends Project implements RisingWaveStreamingRel {
  // TODO: define more attributes corresponding to TableCatalog.

  public RwStreamMaterializedView(
      RelOptCluster cluster,
      RelTraitSet traits,
      List<RelHint> hints,
      RelNode input,
      List<? extends RexNode> projects,
      RelDataType rowType) {
    super(cluster, traits, hints, input, projects, rowType);
    checkConvention();
  }

  @Override
  public Project copy(
      RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
    return new RwStreamMaterializedView(
        getCluster(), traitSet, getHints(), input, projects, rowType);
  }

  @Override
  public StreamNode serialize() {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
