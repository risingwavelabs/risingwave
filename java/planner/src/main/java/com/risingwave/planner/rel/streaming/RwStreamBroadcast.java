package com.risingwave.planner.rel.streaming;

import com.risingwave.proto.streaming.plan.BroadcastNode;
import com.risingwave.proto.streaming.plan.StreamNode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

/** Chain Node */
public class RwStreamBroadcast extends SingleRel implements RisingWaveStreamingRel {

  public RwStreamBroadcast(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
    super(cluster, traits, input);
    checkConvention();
  }

  @Override
  public StreamNode serialize() {
    BroadcastNode broadcastNode = BroadcastNode.newBuilder().build();
    return StreamNode.newBuilder().setBroadcastNode(broadcastNode).build();
  }

  @Override
  public <T> RwStreamingRelVisitor.Result<T> accept(RwStreamingRelVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
